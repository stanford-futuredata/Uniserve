package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

class ServiceBrokerDataStore<R extends Row, S extends Shard> extends BrokerDataStoreGrpc.BrokerDataStoreImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerDataStore.class);
    private final DataStore<R, S> dataStore;

    ServiceBrokerDataStore(DataStore<R, S> dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public StreamObserver<WriteQueryMessage> writeQuery(StreamObserver<WriteQueryResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            long txID;
            WriteQueryPlan<R, S> writeQueryPlan;
            List<R[]> rowArrayList = new ArrayList<>();
            int lastState = DataStore.COLLECT;
            List<R> rows;
            List<StreamObserver<ReplicaWriteMessage>> replicaObservers = new ArrayList<>();
            Semaphore commitSemaphore = new Semaphore(0);
            WriteLockerThread t;

            @Override
            public void onNext(WriteQueryMessage writeQueryMessage) {
                int writeState = writeQueryMessage.getWriteState();
                if (writeState == DataStore.COLLECT) {
                    assert (lastState == DataStore.COLLECT);
                    shardNum = writeQueryMessage.getShard();
                    dataStore.createShardMetadata(shardNum);
                    txID = writeQueryMessage.getTxID();
                    writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(writeQueryMessage.getSerializedQuery()); // TODO:  Only send this once.
                    R[] rowChunk = (R[]) Utilities.byteStringToObject(writeQueryMessage.getRowData());
                    rowArrayList.add(rowChunk);
                } else if (writeState == DataStore.PREPARE) {
                    assert (lastState == DataStore.COLLECT);
                    rows = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                    if (dataStore.shardLockMap.containsKey(shardNum)) {
                        t = new WriteLockerThread(dataStore.shardLockMap.get(shardNum));
                        t.acquireLock();
                        responseObserver.onNext(prepareWriteQuery(shardNum, txID, writeQueryPlan));
                    } else {
                        responseObserver.onNext(WriteQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build());
                    }
                } else if (writeState == DataStore.COMMIT) {
                    assert (lastState == DataStore.PREPARE);
                    commitWriteQuery(shardNum, txID, writeQueryPlan);
                    lastState = writeState;
                    t.releaseLock();
                } else if (writeState == DataStore.ABORT) {
                    assert (lastState == DataStore.PREPARE);
                    abortWriteQuery(shardNum, txID, writeQueryPlan);
                    lastState = writeState;
                    t.releaseLock();
                }
                lastState = writeState;
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("DS{} Primary Write RPC Error Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                if (lastState == DataStore.PREPARE) {
                    if (dataStore.zkCurator.getTransactionStatus(txID) == DataStore.COMMIT) {
                        commitWriteQuery(shardNum, txID, writeQueryPlan);
                    } else {
                        abortWriteQuery(shardNum, txID, writeQueryPlan);
                    }
                    t.releaseLock();
                }
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }

            private WriteQueryResponse prepareWriteQuery(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan) {
                if (dataStore.consistentHash.getBuckets(shardNum).contains(dataStore.dsID)) {
                    dataStore.ensureShardCached(shardNum);
                    S shard;
                    if (dataStore.readWriteAtomicity) {
                        ZKShardDescription z = dataStore.zkCurator.getZKShardDescription(shardNum);
                        if (z == null) {
                            shard = dataStore.shardMap.get(shardNum); // This is the first commit.
                        } else {
                            Optional<S> shardOpt = dataStore.downloadShardFromCloud(shardNum, z.cloudName, z.versionNumber);
                            assert (shardOpt.isPresent());
                            shard = shardOpt.get();
                        }
                        dataStore.multiVersionShardMap.get(shardNum).put(txID, shard);
                    } else {
                        shard = dataStore.shardMap.get(shardNum);
                    }
                    assert(shard != null);
                    List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs =
                            dataStore.replicaDescriptionsMap.get(shardNum).stream().map(i -> i.stub).collect(Collectors.toList());
                    int numReplicas = replicaStubs.size();
                    R[] rowArray;
                    rowArray = (R[]) rows.toArray(new Row[0]);
                    AtomicBoolean success = new AtomicBoolean(true);
                    Semaphore prepareSemaphore = new Semaphore(0);
                    for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub : replicaStubs) {
                        StreamObserver<ReplicaWriteMessage> observer = stub.replicaWrite(new StreamObserver<>() {
                            @Override
                            public void onNext(ReplicaWriteResponse replicaResponse) {
                                if (replicaResponse.getReturnCode() != 0) {
                                    logger.warn("DS{} Replica Prepare Failed Shard {}", dataStore.dsID, shardNum);
                                    success.set(false);
                                }
                                prepareSemaphore.release();
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                logger.warn("DS{} Replica Prepare RPC Failed Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                                success.set(false);
                                prepareSemaphore.release();
                                commitSemaphore.release();
                            }

                            @Override
                            public void onCompleted() {
                                commitSemaphore.release();
                            }
                        });
                        final int stepSize = 10000;
                        for (int i = 0; i < rowArray.length; i += stepSize) {
                            ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                            R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + stepSize));
                            ByteString rowData = Utilities.objectToByteString(rowSlice);
                            ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                                    .setShard(shardNum)
                                    .setSerializedQuery(serializedQuery)
                                    .setRowData(rowData)
                                    .setVersionNumber(dataStore.shardVersionMap.get(shardNum))
                                    .setWriteState(DataStore.COLLECT)
                                    .setTxID(txID)
                                    .build();
                            observer.onNext(rm);
                        }
                        ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                                .setWriteState(DataStore.PREPARE)
                                .build();
                        observer.onNext(rm);
                        replicaObservers.add(observer);
                    }
                    boolean primaryWriteSuccess = writeQueryPlan.preCommit(shard, rows);
                    lastState = DataStore.PREPARE;
                    try {
                        prepareSemaphore.acquire(numReplicas);
                    } catch (InterruptedException e) {
                        logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                        assert (false);
                    }
                    int returnCode;
                    if (primaryWriteSuccess && success.get()) {
                        returnCode = Broker.QUERY_SUCCESS;
                    } else {
                        returnCode = Broker.QUERY_FAILURE;
                    }
                    return WriteQueryResponse.newBuilder().setReturnCode(returnCode).build();
                } else {
                    logger.warn("DS{} Primary got write request for unassigned shard {}", dataStore.dsID, shardNum);
                    t.releaseLock();
                    return WriteQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
                }
            }

            private void commitWriteQuery(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan) {
                S shard;
                if (dataStore.readWriteAtomicity) {
                    shard = dataStore.multiVersionShardMap.get(shardNum).get(txID);
                } else {
                    shard = dataStore.shardMap.get(shardNum);
                }
                for (StreamObserver<ReplicaWriteMessage> observer : replicaObservers) {
                    ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                            .setWriteState(DataStore.COMMIT)
                            .build();
                    observer.onNext(rm);
                    observer.onCompleted();
                }
                writeQueryPlan.commit(shard);
                dataStore.shardMap.put(shardNum, shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
                // Upload the updated shard.
                if (dataStore.dsCloud != null) {
                    dataStore.uploadShardToCloud(shardNum);
                }
                try {
                    commitSemaphore.acquire(replicaObservers.size());
                } catch (InterruptedException e) {
                    logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                    assert (false);
                }
            }

            private void abortWriteQuery(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan) {
                for (StreamObserver<ReplicaWriteMessage> observer : replicaObservers) {
                    ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                            .setWriteState(DataStore.ABORT)
                            .build();
                    observer.onNext(rm);
                    observer.onCompleted();
                }
                if (dataStore.readWriteAtomicity) {
                    S shard = dataStore.multiVersionShardMap.get(shardNum).get(txID);
                    writeQueryPlan.abort(shard);
                    shard.destroy();
                    dataStore.multiVersionShardMap.get(shardNum).remove(txID);
                } else {
                    S shard = dataStore.shardMap.get(shardNum);
                    writeQueryPlan.abort(shard);
                }
                try {
                    commitSemaphore.acquire(replicaObservers.size());
                } catch (InterruptedException e) {
                    logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                    assert (false);
                }
            }

        };
    }
    
    @Override
    public void anchoredReadQuery(AnchoredReadQueryMessage request,  StreamObserver<AnchoredReadQueryResponse> responseObserver) {
        responseObserver.onNext(anchoredReadQueryHandler(request));
        responseObserver.onCompleted();
    }


    private AnchoredReadQueryResponse anchoredReadQueryHandler(AnchoredReadQueryMessage m) {
        AnchoredReadQueryPlan<S, Object> plan =
                (AnchoredReadQueryPlan<S, Object>) Utilities.byteStringToObject(m.getSerializedQuery());
        Map<String, List<Integer>> allTargetShards = (Map<String, List<Integer>>) Utilities.byteStringToObject(m.getTargetShards());
        Map<String, Map<Integer, Integer>> intermediateShards = (Map<String, Map<Integer, Integer>>) Utilities.byteStringToObject(m.getIntermediateShards());
        Map<String, List<ByteString>> ephemeralData = new HashMap<>();
        Map<String, S> ephemeralShards = new HashMap<>();
        int localShardNum = m.getTargetShard();
        String anchorTableName = plan.getAnchorTable();
        dataStore.createShardMetadata(localShardNum);
        dataStore.shardLockMap.get(localShardNum).readerLockLock();
        if (!dataStore.consistentHash.getBuckets(localShardNum).contains(dataStore.dsID)) {
            logger.warn("DS{} Got anchored read request for unassigned local shard {}", dataStore.dsID, localShardNum);
            dataStore.shardLockMap.get(localShardNum).readerLockUnlock();
            return AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
        }
        dataStore.ensureShardCached(localShardNum);
        S localShard;
        long lastCommittedVersion = m.getLastCommittedVersion();
        if (dataStore.readWriteAtomicity) {
            if (dataStore.multiVersionShardMap.get(localShardNum).containsKey(lastCommittedVersion)) {
                localShard = dataStore.multiVersionShardMap.get(localShardNum).get(lastCommittedVersion);
            } else { // TODO: Retrieve the older version from somewhere else?
                logger.info("DS{} missing shard {} version {}", dataStore.dsID, localShardNum, lastCommittedVersion);
                return AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
            }
        } else {
            localShard = dataStore.shardMap.get(localShardNum);
        }
        assert(localShard != null);
        List<Integer> partitionKeys = plan.getPartitionKeys(localShard);
        for (String tableName: plan.getQueriedTables()) {
            if (plan.getQueriedTables().size() > 1) {
                S ephemeralShard = dataStore.createNewShard(dataStore.ephemeralShardNum.decrementAndGet()).get();
                ephemeralShards.put(tableName, ephemeralShard);
            }
            if (!tableName.equals(anchorTableName)) {
                List<Integer> targetShards = allTargetShards.get(tableName);
                List<ByteString> tableEphemeralData = new CopyOnWriteArrayList<>();
                CountDownLatch latch = new CountDownLatch(targetShards.size());
                for (int targetShard : targetShards) {
                    int targetDSID = intermediateShards.containsKey(tableName) ?
                            intermediateShards.get(tableName).get(targetShard) :
                            dataStore.consistentHash.getRandomBucket(targetShard); // TODO:  If it's already here, use it.
                    ManagedChannel channel = dataStore.getChannelForDSID(targetDSID);
                    DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                    AnchoredShuffleMessage g = AnchoredShuffleMessage.newBuilder()
                            .setShardNum(targetShard).setNumReducers(m.getNumReducers()).setReducerShardNum(localShardNum)
                            .setSerializedQuery(m.getSerializedQuery()).setLastCommittedVersion(lastCommittedVersion)
                            .setTxID(m.getTxID()).addAllPartitionKeys(partitionKeys)
                            .setTargetShardIntermediate(intermediateShards.containsKey(tableName)).build();
                    StreamObserver<AnchoredShuffleResponse> responseObserver = new StreamObserver<>() {
                        @Override
                        public void onNext(AnchoredShuffleResponse r) {
                            if (r.getReturnCode() == Broker.QUERY_RETRY) {
                                onError(new Throwable());
                            } else {
                                tableEphemeralData.add(r.getShuffleData());
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.info("DS{}  Shuffle data error shard {}", dataStore.dsID, targetShard);
                            // TODO: First remove all ByteStrings added from this shard.
                            int targetDSID = dataStore.consistentHash.getRandomBucket(targetShard); // TODO:  If it's already here, use it.
                            ManagedChannel channel = dataStore.getChannelForDSID(targetDSID);
                            DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                            stub.anchoredShuffle(g, this);
                        }

                        @Override
                        public void onCompleted() {
                            latch.countDown();
                        }
                    };
                    stub.anchoredShuffle(g, responseObserver);
                }
                try {
                    latch.await();
                } catch (InterruptedException ignored) {
                }
                ephemeralData.put(tableName, tableEphemeralData);
            }
        }
        AnchoredReadQueryResponse r;
        try {
            if (plan.returnTableName().isEmpty()) {
                ByteString b = plan.reducer(localShard, ephemeralData, ephemeralShards);
                r = AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setResponse(b).build();
            } else {
                int intermediateShardNum = dataStore.ephemeralShardNum.decrementAndGet();
                dataStore.createShardMetadata(intermediateShardNum);
                S intermediateShard = dataStore.shardMap.get(intermediateShardNum);
                plan.reducer(localShard, ephemeralData, ephemeralShards, intermediateShard);
                HashMap<Integer, Integer> intermediateShardLocation =
                        new HashMap<>(Map.of(intermediateShardNum, dataStore.dsID));
                ByteString b = Utilities.objectToByteString(intermediateShardLocation);
                r = AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setResponse(b).build();
            }
        } catch (Exception e) {
            logger.warn("Read Query Exception: {}", e.getMessage());
            r = AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
        }
        dataStore.shardLockMap.get(localShardNum).readerLockUnlock();
        ephemeralShards.values().forEach(S::destroy);
        long unixTime = Instant.now().getEpochSecond();
        dataStore.QPSMap.get(localShardNum).merge(unixTime, 1, Integer::sum);
        return r;
    }

    @Override
    public void shuffleReadQuery(ShuffleReadQueryMessage request,  StreamObserver<ShuffleReadQueryResponse> responseObserver) {
        responseObserver.onNext(shuffleReadQueryHandler(request));
        responseObserver.onCompleted();
    }


    private ShuffleReadQueryResponse shuffleReadQueryHandler(ShuffleReadQueryMessage m) {
        ShuffleReadQueryPlan<S, Object> plan =
                (ShuffleReadQueryPlan<S, Object>) Utilities.byteStringToObject(m.getSerializedQuery());
        Map<String, List<Integer>> allTargetShards = (Map<String, List<Integer>>) Utilities.byteStringToObject(m.getTargetShards());
        Map<String, List<ByteString>> ephemeralData = new HashMap<>();
        Map<String, S> ephemeralShards = new HashMap<>();
        for (String tableName: plan.getQueriedTables()) {
            S ephemeralShard = dataStore.createNewShard(dataStore.ephemeralShardNum.decrementAndGet()).get();
            ephemeralShards.put(tableName, ephemeralShard);
            List<Integer> targetShards = allTargetShards.get(tableName);
            List<ByteString> tableEphemeralData = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(targetShards.size());
            for (int targetShard : targetShards) {
                int targetDSID = dataStore.consistentHash.getRandomBucket(targetShard); // TODO:  If it's already here, use it.
                ManagedChannel channel = dataStore.getChannelForDSID(targetDSID);
                DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                ShuffleMessage g = ShuffleMessage.newBuilder()
                        .setShardNum(targetShard).setNumReducers(m.getNumReducers()).setReducerNum(m.getReducerNum())
                        .setSerializedQuery(m.getSerializedQuery())
                        .setTxID(m.getTxID()).build();
                StreamObserver<ShuffleResponse> responseObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(ShuffleResponse r) {
                        if (r.getReturnCode() == Broker.QUERY_RETRY) {
                            onError(new Throwable());
                        } else {
                            tableEphemeralData.add(r.getShuffleData());
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.info("DS{}  Shuffle data error shard {}", dataStore.dsID, targetShard);
                        // TODO: First remove all ByteStrings added from this shard.
                        int targetDSID = dataStore.consistentHash.getRandomBucket(targetShard); // TODO:  If it's already here, use it.
                        ManagedChannel channel = dataStore.getChannelForDSID(targetDSID);
                        DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                        stub.shuffle(g, this);
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                };
                stub.shuffle(g, responseObserver);
            }
            try {
                latch.await();
            } catch (InterruptedException ignored) {
            }
            ephemeralData.put(tableName, tableEphemeralData);

        }
        ByteString b = plan.reducer(ephemeralData, ephemeralShards);
        ephemeralShards.values().forEach(S::destroy);
        return ShuffleReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setResponse(b).build();
    }
}
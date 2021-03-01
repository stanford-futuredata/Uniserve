package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class ServiceDataStoreDataStore<R extends Row, S extends Shard> extends DataStoreDataStoreGrpc.DataStoreDataStoreImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerDataStore.class);
    private final DataStore<R, S> dataStore;

    ServiceDataStoreDataStore(DataStore<R, S> dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void bootstrapReplica(BootstrapReplicaMessage request, StreamObserver<BootstrapReplicaResponse> responseObserver) {
        responseObserver.onNext(bootstrapReplicaHandler(request));
        responseObserver.onCompleted();
    }

    private BootstrapReplicaResponse bootstrapReplicaHandler(BootstrapReplicaMessage request) {
        int shardNum = request.getShard();
        dataStore.shardLockMap.get(shardNum).writerLockLock();
        Integer replicaVersion = request.getVersionNumber();
        Integer primaryVersion = dataStore.shardVersionMap.get(shardNum);
        assert(primaryVersion != null);
        assert(replicaVersion <= primaryVersion);
        assert(dataStore.shardMap.containsKey(shardNum));  // TODO: Could fail during shard transfers?
        Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
        if (replicaVersion.equals(primaryVersion)) {
            DataStoreDescription dsDescription = dataStore.zkCurator.getDSDescription(request.getDsID());
            ManagedChannel channel = ManagedChannelBuilder.forAddress(dsDescription.host, dsDescription.port).usePlaintext().build();
            DataStoreDataStoreGrpc.DataStoreDataStoreStub asyncStub = DataStoreDataStoreGrpc.newStub(channel);
            ReplicaDescription rd = new ReplicaDescription(request.getDsID(), channel, asyncStub);
            dataStore.replicaDescriptionsMap.get(shardNum).add(rd);
        }
        dataStore.shardLockMap.get(shardNum).writerLockUnlock();
        List<WriteQueryPlan<R, S>> writeQueryPlans = new ArrayList<>();
        List<R[]> rowListList = new ArrayList<>();
        for (int v = replicaVersion + 1; v <= primaryVersion; v++) {
            writeQueryPlans.add(shardWriteLog.get(v).getValue0());
            rowListList.add((R[]) shardWriteLog.get(v).getValue1().toArray(new Row[0]));
        }
        WriteQueryPlan<R, S>[] writeQueryPlansArray = writeQueryPlans.toArray(new WriteQueryPlan[0]);
        R[][] rowArrayArray = rowListList.toArray((R[][]) new Row[0][]);
        return BootstrapReplicaResponse.newBuilder().setReturnCode(0)
                .setVersionNumber(primaryVersion)
                .setWriteData(Utilities.objectToByteString(rowArrayArray))
                .setWriteQueries(Utilities.objectToByteString(writeQueryPlansArray))
                .build();
    }

    @Override
    public StreamObserver<ReplicaWriteMessage> replicaWrite(StreamObserver<ReplicaWriteResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            int versionNumber;
            long txID;
            WriteQueryPlan<R, S> writeQueryPlan;
            final List<R[]> rowArrayList = new ArrayList<>();
            List<R> rowList;
            int lastState = DataStore.COLLECT;
            WriteLockerThread t;

            @Override
            public void onNext(ReplicaWriteMessage replicaWriteMessage) {
                int writeState = replicaWriteMessage.getWriteState();
                if (writeState == DataStore.COLLECT) {
                    assert(lastState == DataStore.COLLECT);
                    versionNumber = replicaWriteMessage.getVersionNumber();
                    shardNum = replicaWriteMessage.getShard();
                    txID = replicaWriteMessage.getTxID();
                    writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(replicaWriteMessage.getSerializedQuery()); // TODO:  Only send this once.
                    R[] rowChunk = (R[]) Utilities.byteStringToObject(replicaWriteMessage.getRowData());
                    rowArrayList.add(rowChunk);
                } else if (writeState == DataStore.PREPARE) {
                    assert(lastState == DataStore.COLLECT);
                    rowList = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                    t = new WriteLockerThread(dataStore.shardLockMap.get(shardNum));
                    t.acquireLock();
                    // assert(versionNumber == dataStore.shardVersionMap.get(shardNum));
                    responseObserver.onNext(prepareReplicaWrite(shardNum, writeQueryPlan, rowList));
                    lastState = writeState;
                } else if (writeState == DataStore.COMMIT) {
                    assert(lastState == DataStore.PREPARE);
                    commitReplicaWrite(shardNum, writeQueryPlan, rowList);
                    lastState = writeState;
                    t.releaseLock();
                } else if (writeState == DataStore.ABORT) {
                    assert(lastState == DataStore.PREPARE);
                    abortReplicaWrite(shardNum, writeQueryPlan);
                    lastState = writeState;
                    t.releaseLock();
                }
                lastState = writeState;
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("DS{} Replica RPC Error Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                // TODO:  What if the primary fails after reporting a successful prepare but before the commit?
                if (lastState == DataStore.PREPARE) {
                    if (dataStore.zkCurator.getTransactionStatus(txID) == DataStore.COMMIT) {
                        commitReplicaWrite(shardNum, writeQueryPlan, rowList);
                    } else {
                        abortReplicaWrite(shardNum, writeQueryPlan);
                    }
                    t.releaseLock();
                }
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }

            private ReplicaWriteResponse prepareReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
                if (dataStore.shardMap.containsKey(shardNum)) {
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
                    boolean success =  writeQueryPlan.preCommit(shard, rows);
                    if (success) {
                        return ReplicaWriteResponse.newBuilder().setReturnCode(0).build();
                    } else {
                        return ReplicaWriteResponse.newBuilder().setReturnCode(1).build();
                    }
                } else {
                    logger.warn("DS{} replica got write request for absent shard {}", dataStore.dsID, shardNum);
                    return ReplicaWriteResponse.newBuilder().setReturnCode(1).build();
                }
            }

            private void commitReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {                S shard;
                if (dataStore.readWriteAtomicity) {
                    shard = dataStore.multiVersionShardMap.get(shardNum).get(txID);
                } else {
                    shard = dataStore.shardMap.get(shardNum);
                }
                writeQueryPlan.commit(shard);
                dataStore.shardMap.put(shardNum, shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
            }

            private void abortReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan) {
                if (dataStore.readWriteAtomicity) {
                    S shard = dataStore.multiVersionShardMap.get(shardNum).get(txID);
                    writeQueryPlan.abort(shard);
                    shard.destroy();
                    dataStore.multiVersionShardMap.get(shardNum).remove(txID);
                } else {
                    S shard = dataStore.shardMap.get(shardNum);
                    writeQueryPlan.abort(shard);
                }
            }
        };
    }

    @Override
    public void dataStorePing(DataStorePingMessage request, StreamObserver<DataStorePingResponse> responseObserver) {
        responseObserver.onNext(DataStorePingResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private final Map<Pair<Long, Integer>, Semaphore> txSemaphores = new ConcurrentHashMap<>();
    private final Map<Pair<Long, Integer>, Map<Integer, List<ByteString>>> txShuffledData = new ConcurrentHashMap<>();
    private final Map<Pair<Long, Integer>, Map<Integer, List<Integer>>> txPartitionKeys = new ConcurrentHashMap<>();
    private final Map<Pair<Long, Integer>, AtomicInteger> txCounts = new ConcurrentHashMap<>();

    @Override
    public void anchoredShuffle(AnchoredShuffleMessage m, StreamObserver<AnchoredShuffleResponse> responseObserver) {
        long txID = m.getTxID();
        int shardNum = m.getShardNum();
        AnchoredReadQueryPlan<S, Object> plan = (AnchoredReadQueryPlan<S, Object>) Utilities.byteStringToObject(m.getSerializedQuery());
        Pair<Long, Integer> mapID = new Pair<>(txID, shardNum);
        dataStore.createShardMetadata(shardNum);
        dataStore.shardLockMap.get(shardNum).readerLockLock();
        if (!dataStore.consistentHash.getBuckets(shardNum).contains(dataStore.dsID)) {
            logger.warn("DS{} Got read request for unassigned shard {}", dataStore.dsID, shardNum);
            dataStore.shardLockMap.get(shardNum).readerLockUnlock();
            responseObserver.onNext(AnchoredShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build());
            responseObserver.onCompleted();
            return;
        }

        txPartitionKeys.computeIfAbsent(mapID, k -> new ConcurrentHashMap<>()).put(m.getReducerShardNum(), m.getPartitionKeysList());
        Semaphore s = txSemaphores.computeIfAbsent(mapID, k -> new Semaphore(0));
        if (txCounts.computeIfAbsent(mapID, k -> new AtomicInteger(0)).incrementAndGet() == m.getNumReducers()) {
            dataStore.ensureShardCached(shardNum);
            S shard;
            if (dataStore.readWriteAtomicity) {
                long lastCommittedVersion = m.getLastCommittedVersion();
                if (dataStore.multiVersionShardMap.get(shardNum).containsKey(lastCommittedVersion)) {
                    shard = dataStore.multiVersionShardMap.get(shardNum).get(lastCommittedVersion);
                } else { // TODO: Retrieve the older version from somewhere else?
                    logger.info("DS{} missing shard {} version {}", dataStore.dsID, shardNum, lastCommittedVersion);
                    responseObserver.onNext(AnchoredShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build());
                    responseObserver.onCompleted();
                    return;
                }
            } else {
                shard = dataStore.shardMap.get(shardNum);
            }
            assert (shard != null);
            Map<Integer, List<ByteString>> mapperResult = plan.mapper(shard, txPartitionKeys.get(mapID));
            txShuffledData.put(mapID, mapperResult);
            // long unixTime = Instant.now().getEpochSecond();
            // dataStore.QPSMap.get(shardNum).merge(unixTime, 1, Integer::sum);
            s.release(m.getNumReducers() - 1);
        } else {
            s.acquireUninterruptibly();
        }
        Map<Integer, List<ByteString>> mapperResult = txShuffledData.get(mapID);
        assert(mapperResult.containsKey(m.getReducerShardNum()));
        List<ByteString> ephemeralData = mapperResult.get(m.getReducerShardNum());
        mapperResult.remove(m.getReducerShardNum());  // TODO: Make reliable--what if map immutable?.
        if (mapperResult.isEmpty()) {
            txShuffledData.remove(mapID);
        }
        dataStore.shardLockMap.get(shardNum).readerLockUnlock();
        for (ByteString item: ephemeralData) {
            responseObserver.onNext(AnchoredShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setShuffleData(item).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void shuffle(ShuffleMessage m, StreamObserver<ShuffleResponse> responseObserver) {
        long txID = m.getTxID();
        int shardNum = m.getShardNum();
        ShuffleReadQueryPlan<S, Object> plan = (ShuffleReadQueryPlan<S, Object>) Utilities.byteStringToObject(m.getSerializedQuery());
        Pair<Long, Integer> mapID = new Pair<>(txID, shardNum);
        dataStore.createShardMetadata(shardNum);
        dataStore.shardLockMap.get(shardNum).readerLockLock();
        if (!dataStore.consistentHash.getBuckets(shardNum).contains(dataStore.dsID)) {
            logger.warn("DS{} Got read request for unassigned shard {}", dataStore.dsID, shardNum);
            dataStore.shardLockMap.get(shardNum).readerLockUnlock();
            responseObserver.onNext(ShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build());
            responseObserver.onCompleted();
            return;
        }
        Semaphore s = txSemaphores.computeIfAbsent(mapID, k -> new Semaphore(0));
        if (txCounts.computeIfAbsent(mapID, k -> new AtomicInteger(0)).compareAndSet(0, 1)) {
            dataStore.ensureShardCached(shardNum);
            S shard = dataStore.shardMap.get(shardNum);
            assert (shard != null);
            Map<Integer, List<ByteString>> mapperResult = plan.mapper(shard, m.getNumReducers());
            txShuffledData.put(mapID, mapperResult);
            long unixTime = Instant.now().getEpochSecond();
            dataStore.QPSMap.get(shardNum).merge(unixTime, 1, Integer::sum);
            s.release(m.getNumReducers() - 1);
        } else {
            s.acquireUninterruptibly();
        }
        Map<Integer, List<ByteString>> mapperResult = txShuffledData.get(mapID);
        assert(mapperResult.containsKey(m.getReducerNum()));
        List<ByteString> ephemeralData = mapperResult.get(m.getReducerNum());
        mapperResult.remove(m.getReducerNum());  // TODO: Make reliable--what if map immutable?.
        if (mapperResult.isEmpty()) {
            txShuffledData.remove(mapID);
        }
        dataStore.shardLockMap.get(shardNum).readerLockUnlock();
        for (ByteString item: ephemeralData) {
            responseObserver.onNext(ShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setShuffleData(item).build());
        }
        responseObserver.onCompleted();
    }
}

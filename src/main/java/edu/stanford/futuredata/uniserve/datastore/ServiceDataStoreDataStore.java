package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
        dataStore.shardLockMap.get(shardNum).writerLockLock(-1);
        Integer replicaVersion = request.getVersionNumber();
        Integer primaryVersion = dataStore.shardVersionMap.get(shardNum);
        assert(primaryVersion != null);
        assert(replicaVersion <= primaryVersion);
        assert(dataStore.primaryShardMap.containsKey(shardNum));  // TODO: Could fail during shard transfers?
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
        return new PreemptibleStreamObserver<>() {
            int shardNum;
            int versionNumber;
            long txID;
            WriteQueryPlan<R, S> writeQueryPlan;
            List<R[]> rowArrayList = new ArrayList<>();
            List<R> rowList;
            int lastState = DataStore.COLLECT;
            WriteLockerThread t;
            private Lock preemptionLock = new ReentrantLock();

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
                    t = new WriteLockerThread(dataStore.shardLockMap.get(shardNum), this, dataStore.dsID, shardNum, txID);
                    preemptionLock.lock();
                    t.acquireLock();
                    // assert(versionNumber == dataStore.shardVersionMap.get(shardNum));
                    responseObserver.onNext(prepareReplicaWrite(shardNum, writeQueryPlan, rowList));
                    lastState = writeState;
                    preemptionLock.unlock();
                } else if (writeState == DataStore.COMMIT) {
                    assert(lastState == DataStore.PREPARE);
                    preemptionLock.lock();
                    commitReplicaWrite(shardNum, writeQueryPlan, rowList);
                    lastState = writeState;
                    preemptionLock.unlock();
                    t.releaseLock();
                } else if (writeState == DataStore.ABORT) {
                    assert(lastState == DataStore.PREPARE);
                    preemptionLock.lock();
                    abortReplicaWrite(shardNum, writeQueryPlan);
                    lastState = writeState;
                    preemptionLock.unlock();
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


            @Override
            public boolean preempt() {
                if (!preemptionLock.tryLock()) {
                    return false;
                }
                if (lastState == DataStore.PREPARE) {
                    abortReplicaWrite(shardNum, writeQueryPlan);
                    return true;
                } else {
                    assert(lastState == DataStore.COMMIT || lastState == DataStore.ABORT);
                    preemptionLock.unlock();
                    return false;
                }
            }

            @Override
            public void resume() {
                ReplicaWriteResponse r = prepareReplicaWrite(shardNum, writeQueryPlan, rowList);
                assert(r.getReturnCode() == Broker.QUERY_SUCCESS); // TODO:  What if it fails?
                preemptionLock.unlock();
            }

            @Override
            public long getTXID() {
                return txID;
            }

            private ReplicaWriteResponse prepareReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
                if (dataStore.replicaShardMap.containsKey(shardNum)) {
                    S shard = dataStore.replicaShardMap.get(shardNum);
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

            private void commitReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
                S shard = dataStore.replicaShardMap.get(shardNum);
                writeQueryPlan.commit(shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
            }

            private void abortReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan) {
                S shard = dataStore.replicaShardMap.get(shardNum);
                writeQueryPlan.abort(shard);
            }
        };
    }

    @Override
    public void dataStorePing(DataStorePingMessage request, StreamObserver<DataStorePingResponse> responseObserver) {
        responseObserver.onNext(DataStorePingResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}

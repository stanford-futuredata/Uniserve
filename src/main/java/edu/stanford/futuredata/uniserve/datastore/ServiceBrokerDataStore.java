package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

class ServiceBrokerDataStore<R extends Row, S extends Shard> extends BrokerDataStoreGrpc.BrokerDataStoreImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerDataStore.class);
    private final DataStore<R, S> dataStore;

    ServiceBrokerDataStore(DataStore<R, S> dataStore) {
        this.dataStore = dataStore;
    }

    private Map<Integer, CommitLockerThread<R, S>> activeCLTs = new HashMap<>();

    @Override
    public StreamObserver<WriteQueryPreCommitMessage> writeQueryPreCommit(StreamObserver<WriteQueryPreCommitResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            long txID;
            WriteQueryPlan<R, S> writeQueryPlan;
            List<R[]> rowArrayList = new ArrayList<>();

            @Override
            public void onNext(WriteQueryPreCommitMessage writeQueryPreCommitMessage) {
                shardNum = writeQueryPreCommitMessage.getShard();
                txID = writeQueryPreCommitMessage.getTxID();
                writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(writeQueryPreCommitMessage.getSerializedQuery()); // TODO:  Only send this once.
                R[] rowChunk = (R[]) Utilities.byteStringToObject(writeQueryPreCommitMessage.getRowData());
                rowArrayList.add(rowChunk);
            }

            @Override
            public void onError(Throwable throwable) {
                assert (false);
            }

            @Override
            public void onCompleted() {
                List<R> rowList = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                responseObserver.onNext(writeQueryPreCommitHandler(shardNum, txID, writeQueryPlan, rowList));
                responseObserver.onCompleted();
            }
        };
    }

    private WriteQueryPreCommitResponse writeQueryPreCommitHandler(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
        // Use the CommitLockerThread to acquire the shard's write lock.
        CommitLockerThread<R, S> commitLockerThread = new CommitLockerThread<>(activeCLTs, shardNum, writeQueryPlan,
                rows, dataStore.shardLockMap.get(shardNum).writeLock(), txID, dataStore.dsID);
        commitLockerThread.acquireLock();
        if (dataStore.primaryShardMap.containsKey(shardNum)) {
            S shard = dataStore.primaryShardMap.get(shardNum);
            List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs = dataStore.replicaDescriptionsMap.get(shardNum).stream().map(i -> i.stub).collect(Collectors.toList());
            int numReplicas = replicaStubs.size();
            R[] rowArray;
            rowArray = (R[]) rows.toArray(new Row[0]);
            AtomicBoolean success = new AtomicBoolean(true);
            Semaphore semaphore = new Semaphore(0);
            for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub: replicaStubs) {
                StreamObserver<ReplicaPreCommitMessage> observer = stub.replicaPreCommit(new StreamObserver<>() {
                    @Override
                    public void onNext(ReplicaPreCommitResponse replicaPreCommitResponse) {
                        if (replicaPreCommitResponse.getReturnCode() != 0) {
                            logger.warn("Replica PreCommit Failed");
                            success.set(false);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        assert(false);
                    }

                    @Override
                    public void onCompleted() {
                        semaphore.release();
                    }
                });
                final int STEPSIZE = 10000;
                for(int i = 0; i < rowArray.length; i += STEPSIZE) {
                    ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                    R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    ReplicaPreCommitMessage rm = ReplicaPreCommitMessage.newBuilder()
                            .setShard(shardNum)
                            .setSerializedQuery(serializedQuery)
                            .setRowData(rowData)
                            .setTxID(txID)
                            .setVersionNumber(dataStore.shardVersionMap.get(shardNum))
                            .build();
                    observer.onNext(rm);
                }
                observer.onCompleted();
            }
            boolean primaryWriteSuccess = writeQueryPlan.preCommit(shard, rows);
            try {
                semaphore.acquire(numReplicas);
            } catch (InterruptedException e) {
                logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                assert(false);
            }
            int addRowReturnCode;
            if (primaryWriteSuccess && success.get()) {
                addRowReturnCode = Broker.QUERY_SUCCESS;
            } else {
                addRowReturnCode = Broker.QUERY_FAILURE;
            }
            return WriteQueryPreCommitResponse.newBuilder().setReturnCode(addRowReturnCode).build();
        } else {
            commitLockerThread.releaseLock();
            logger.warn("DS{} Primary got write request for absent shard {}", dataStore.dsID, shardNum);
            return WriteQueryPreCommitResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
        }
    }

    @Override
    public void writeQueryCommit(WriteQueryCommitMessage request, StreamObserver<WriteQueryCommitResponse> responseObserver) {
        responseObserver.onNext(writeQueryCommitHandler(request));
        responseObserver.onCompleted();
    }

    private WriteQueryCommitResponse writeQueryCommitHandler(WriteQueryCommitMessage rowMessage) {
        int shardNum = rowMessage.getShard();
        CommitLockerThread<R, S> commitCLT = activeCLTs.get(shardNum);
        assert(commitCLT != null);  // The commit locker thread holds the shard's write lock.
        assert(commitCLT.txID == rowMessage.getTxID());
        WriteQueryPlan<R, S> writeQueryPlan = commitCLT.writeQueryPlan;
        if (dataStore.primaryShardMap.containsKey(shardNum)) {
            boolean commitOrAbort = rowMessage.getCommitOrAbort(); // Commit on true, abort on false.
            S shard = dataStore.primaryShardMap.get(shardNum);

            List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs = dataStore.replicaDescriptionsMap.get(shardNum).stream().map(i -> i.stub).collect(Collectors.toList());
            int numReplicas = replicaStubs.size();
            ReplicaCommitMessage rm = ReplicaCommitMessage.newBuilder()
                    .setShard(shardNum)
                    .setCommitOrAbort(commitOrAbort)
                    .setTxID(rowMessage.getTxID())
                    .build();
            Semaphore semaphore = new Semaphore(0);
            for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub: replicaStubs) {
                StreamObserver<ReplicaCommitResponse> responseObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(ReplicaCommitResponse replicaCommitResponse) {
                        semaphore.release();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("DS{} Replica Commit Error: {}", dataStore.dsID, throwable.getMessage());
                        semaphore.release();
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
                stub.replicaCommit(rm, responseObserver);
            }
            if (commitOrAbort) {
                writeQueryPlan.commit(shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, commitCLT.rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
            } else {
                writeQueryPlan.abort(shard);
            }
            try {
                semaphore.acquire(numReplicas);
            } catch (InterruptedException e) {
                logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                assert(false);
            }
            // Have the commit locker thread release the shard's write lock.
            commitCLT.releaseLock();
        } else {
            logger.error("DS{} Got valid commit request on absent shard {} (!!!!!)", dataStore.dsID, shardNum);
            assert(false);
        }
        return WriteQueryCommitResponse.newBuilder().build();
    }

    @Override
    public void readQuery(ReadQueryMessage request,  StreamObserver<ReadQueryResponse> responseObserver) {
        responseObserver.onNext(readQueryHandler(request));
        responseObserver.onCompleted();
    }

    private ReadQueryResponse readQueryHandler(ReadQueryMessage readQuery) {
        int shardNum = readQuery.getShard();
        dataStore.shardLockMap.get(shardNum).readLock().lock();
        S shard = dataStore.replicaShardMap.getOrDefault(shardNum, null);
        if (shard == null) {
            shard = dataStore.primaryShardMap.getOrDefault(shardNum, null);
        }
        if (shard != null) {
            ByteString serializedQuery = readQuery.getSerializedQuery();
            ReadQueryPlan<S, Serializable, Object> readQueryPlan;
            readQueryPlan = (ReadQueryPlan<S, Serializable, Object>) Utilities.byteStringToObject(serializedQuery);
            Serializable queryResult = readQueryPlan.queryShard(shard);
            dataStore.shardLockMap.get(shardNum).readLock().unlock();
            ByteString queryResponse;
            queryResponse = Utilities.objectToByteString(queryResult);
            return ReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setResponse(queryResponse).build();
        } else {
            dataStore.shardLockMap.get(shardNum).readLock().unlock();
            logger.warn("DS{} Got read request for absent shard {}", dataStore.dsID, shardNum);
            return ReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
        }
    }
}
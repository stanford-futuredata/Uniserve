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

import java.time.Instant;
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

    @Override
    public StreamObserver<WriteQueryMessage> writeQuery(StreamObserver<WriteQueryResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            long txID;
            WriteQueryPlan<R, S> writeQueryPlan;
            List<R[]> rowArrayList = new ArrayList<>();

            @Override
            public void onNext(WriteQueryMessage writeQueryMessage) {
                shardNum = writeQueryMessage.getShard();
                txID = writeQueryMessage.getTxID();
                writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(writeQueryMessage.getSerializedQuery()); // TODO:  Only send this once.
                R[] rowChunk = (R[]) Utilities.byteStringToObject(writeQueryMessage.getRowData());
                rowArrayList.add(rowChunk);
            }

            @Override
            public void onError(Throwable throwable) {
                assert (false);
            }

            @Override
            public void onCompleted() {
                List<R> rowList = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                responseObserver.onNext(writeQueryHandler(shardNum, txID, writeQueryPlan, rowList));
                responseObserver.onCompleted();
            }
        };
    }

    private WriteQueryResponse writeQueryHandler(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
        dataStore.shardLockMap.get(shardNum).writerLockLock();
        if (dataStore.primaryShardMap.containsKey(shardNum)) {
            S shard = dataStore.primaryShardMap.get(shardNum);
            List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs = dataStore.replicaDescriptionsMap.get(shardNum).stream().map(i -> i.stub).collect(Collectors.toList());
            int numReplicas = replicaStubs.size();
            R[] rowArray;
            rowArray = (R[]) rows.toArray(new Row[0]);
            AtomicBoolean success = new AtomicBoolean(true);
            Semaphore prepareSemaphore = new Semaphore(0);
            Semaphore commitSemaphore = new Semaphore(0);
            List<StreamObserver<ReplicaWriteMessage>> replicaObservers = new ArrayList<>();
            for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub: replicaStubs) {
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
                replicaObservers.add(observer);
            }
            for (StreamObserver<ReplicaWriteMessage> observer: replicaObservers) {
                final int STEPSIZE = 10000;
                for(int i = 0; i < rowArray.length; i += STEPSIZE) {
                    ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                    R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                            .setShard(shardNum)
                            .setSerializedQuery(serializedQuery)
                            .setRowData(rowData)
                            .setVersionNumber(dataStore.shardVersionMap.get(shardNum))
                            .setWriteState(DataStore.COLLECT)
                            .build();
                    observer.onNext(rm);
                }
                ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                        .setWriteState(DataStore.PREPARE)
                        .build();
                observer.onNext(rm);
            }
            boolean primaryWriteSuccess = writeQueryPlan.preCommit(shard, rows);
            try {
                prepareSemaphore.acquire(numReplicas);
            } catch (InterruptedException e) {
                logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                assert(false);
            }
            int returnCode;
            if (primaryWriteSuccess && success.get()) {
                for (StreamObserver<ReplicaWriteMessage> observer: replicaObservers) {
                    ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                            .setWriteState(DataStore.COMMIT)
                            .build();
                    observer.onNext(rm);
                    observer.onCompleted();
                }
                writeQueryPlan.commit(shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
                returnCode = Broker.QUERY_SUCCESS;
            } else {
                for (StreamObserver<ReplicaWriteMessage> observer: replicaObservers) {
                    ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                            .setWriteState(DataStore.ABORT)
                            .build();
                    observer.onNext(rm);
                    observer.onCompleted();
                }
                writeQueryPlan.abort(shard);
                returnCode = Broker.QUERY_FAILURE;
            }
            try {
                commitSemaphore.acquire(numReplicas);
            } catch (InterruptedException e) {
                logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                assert(false);
            }
            dataStore.shardLockMap.get(shardNum).writerLockUnlock();
            return WriteQueryResponse.newBuilder().setReturnCode(returnCode).build();
        } else {
            dataStore.shardLockMap.get(shardNum).writerLockUnlock();
            logger.warn("DS{} Primary got write request for absent shard {}", dataStore.dsID, shardNum);
            return WriteQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
        }
    }

    @Override
    public void readQuery(ReadQueryMessage request,  StreamObserver<ReadQueryResponse> responseObserver) {
        responseObserver.onNext(readQueryHandler(request));
        responseObserver.onCompleted();
    }

    private ReadQueryResponse readQueryHandler(ReadQueryMessage readQuery) {
        long fullStartTime = System.nanoTime();
        int shardNum = readQuery.getShard();
        dataStore.shardLockMap.get(shardNum).readerLockLock();
        long unixTime = Instant.now().getEpochSecond();
        dataStore.QPSMap.get(shardNum).compute(unixTime, (k, v) -> v == null ? 1 : v + 1);
        S shard = dataStore.replicaShardMap.getOrDefault(shardNum, null);
        if (shard == null) {
            shard = dataStore.primaryShardMap.getOrDefault(shardNum, null);
        }
        if (shard != null) {
            ByteString serializedQuery = readQuery.getSerializedQuery();
            ReadQueryPlan<S, Object> readQueryPlan;
            readQueryPlan = (ReadQueryPlan<S, Object>) Utilities.byteStringToObject(serializedQuery);
            long executeStartTime = System.nanoTime();
            ByteString queryResponse = readQueryPlan.queryShard(shard);
            long executeEndTime = System.nanoTime();
            dataStore.readQueryExecuteTimes.add((executeEndTime - executeStartTime) / 1000L);
            dataStore.shardLockMap.get(shardNum).readerLockUnlock();
            long fullEndtime = System.nanoTime();
            dataStore.readQueryFullTimes.add((fullEndtime - fullStartTime) / 1000L);
            return ReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setResponse(queryResponse).build();
        } else {
            dataStore.shardLockMap.get(shardNum).readerLockUnlock();
            logger.warn("DS{} Got read request for absent shard {}", dataStore.dsID, shardNum);
            return ReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
        }
    }
}
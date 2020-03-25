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
        dataStore.shardLockMap.get(shardNum).writerLockLock();
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
                            logger.warn("DS{} Replica PreCommit Failed Shard {}", dataStore.dsID, shardNum);
                            success.set(false);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.warn("DS{} Replica PreCommit RPC Failed Shard {}", dataStore.dsID, shardNum);
                        semaphore.release();  // TODO:  Maybe some kind of retry or failure needed?
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
                writeQueryPlan.commit(shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
            } else {
                addRowReturnCode = Broker.QUERY_FAILURE;
            }
            dataStore.shardLockMap.get(shardNum).writerLockUnlock();
            return WriteQueryPreCommitResponse.newBuilder().setReturnCode(addRowReturnCode).build();
        } else {
            dataStore.shardLockMap.get(shardNum).writerLockUnlock();
            logger.warn("DS{} Primary got write request for absent shard {}", dataStore.dsID, shardNum);
            return WriteQueryPreCommitResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
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
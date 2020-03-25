package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.*;
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
        dataStore.shardLockMap.get(shardNum).readLock().lock();
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
        dataStore.shardLockMap.get(shardNum).readLock().unlock();
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
    public StreamObserver<ReplicaPreCommitMessage> replicaPreCommit(StreamObserver<ReplicaPreCommitResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            long txID;
            int versionNumber;
            WriteQueryPlan<R, S> writeQueryPlan;
            List<R[]> rowArrayList = new ArrayList<>();

            @Override
            public void onNext(ReplicaPreCommitMessage replicaPreCommitMessage) {
                versionNumber = replicaPreCommitMessage.getVersionNumber();
                shardNum = replicaPreCommitMessage.getShard();
                txID = replicaPreCommitMessage.getTxID();
                writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(replicaPreCommitMessage.getSerializedQuery()); // TODO:  Only send this once.
                R[] rowChunk = (R[]) Utilities.byteStringToObject(replicaPreCommitMessage.getRowData());
                rowArrayList.add(rowChunk);
            }

            @Override
            public void onError(Throwable throwable) {
                assert (false);
            }

            @Override
            public void onCompleted() {
                List<R> rowList = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                responseObserver.onNext(replicaPreCommitHandler(shardNum, txID, writeQueryPlan, rowList, versionNumber));
                responseObserver.onCompleted();
            }
        };
    }

    private ReplicaPreCommitResponse replicaPreCommitHandler(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows, int versionNumber) {
        // Use the CommitLockerThread to acquire the shard's write lock.
        dataStore.shardLockMap.get(shardNum).writeLock().lock();
        if (dataStore.replicaShardMap.containsKey(shardNum)) {
            S shard = dataStore.replicaShardMap.get(shardNum);
            assert(versionNumber == dataStore.shardVersionMap.get(shardNum));
            boolean replicaWriteSuccess = writeQueryPlan.preCommit(shard, rows);
            int returnCode;
            if (replicaWriteSuccess) {
                returnCode = 0;
                writeQueryPlan.commit(shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
            } else {
                returnCode = 1;
            }
            dataStore.shardLockMap.get(shardNum).writeLock().unlock();
            return ReplicaPreCommitResponse.newBuilder().setReturnCode(returnCode).build();
        } else {
            dataStore.shardLockMap.get(shardNum).writeLock().unlock();
            logger.warn("DS{} replica got write request for absent shard {}", dataStore.dsID, shardNum);
            return ReplicaPreCommitResponse.newBuilder().setReturnCode(1).build();
        }
    }

    @Override
    public void dataStorePing(DataStorePingMessage request, StreamObserver<DataStorePingResponse> responseObserver) {
        responseObserver.onNext(DataStorePingResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}

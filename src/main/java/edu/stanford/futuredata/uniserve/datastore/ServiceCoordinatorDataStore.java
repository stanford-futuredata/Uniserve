package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ServiceCoordinatorDataStore<R extends Row, S extends Shard> extends CoordinatorDataStoreGrpc.CoordinatorDataStoreImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerDataStore.class);
    private final DataStore<R, S> dataStore;

    ServiceCoordinatorDataStore(DataStore<R, S> dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void createNewShard(CreateNewShardMessage request, StreamObserver<CreateNewShardResponse> responseObserver) {
        responseObserver.onNext(createNewShardHandler(request));
        responseObserver.onCompleted();
    }

    private CreateNewShardResponse createNewShardHandler(CreateNewShardMessage request) {
        int shardNum = request.getShard();
        assert (!dataStore.primaryShardMap.containsKey(shardNum));
        assert (!dataStore.replicaShardMap.containsKey(shardNum));
        Path shardPath = Path.of(dataStore.baseDirectory.toString(), Integer.toString(0), Integer.toString(shardNum));
        File shardPathFile = shardPath.toFile();
        if (!shardPathFile.exists()) {
            boolean mkdirs = shardPathFile.mkdirs();
            if (!mkdirs) {
                logger.error("DS{} Shard directory creation failed {}", dataStore.dsID, shardNum);
                return CreateNewShardResponse.newBuilder().setReturnCode(1).build();
            }
        }
        Optional<S> shard = dataStore.shardFactory.createNewShard(shardPath);
        if (shard.isEmpty()) {
            logger.error("DS{} Shard creation failed {}", dataStore.dsID, shardNum);
            return CreateNewShardResponse.newBuilder().setReturnCode(1).build();
        }
        dataStore.shardLockMap.put(shardNum, new ReentrantReadWriteLock());
        dataStore.shardVersionMap.put(shardNum, 0);
        dataStore.lastUploadedVersionMap.put(shardNum, 0);
        dataStore.writeLog.put(shardNum, new ConcurrentHashMap<>());
        dataStore.replicaChannelsMap.put(shardNum, new ArrayList<>());
        dataStore.replicaStubsMap.put(shardNum, new ArrayList<>());
        dataStore.primaryShardMap.put(shardNum, shard.get());
        logger.info("DS{} Created new primary shard {}", dataStore.dsID, shardNum);
        return CreateNewShardResponse.newBuilder().setReturnCode(0).build();
    }

    @Override
    public void loadShardReplica(LoadShardReplicaMessage request, StreamObserver<LoadShardReplicaResponse> responseObserver) {
        responseObserver.onNext(loadShardReplicaHandler(request));
        responseObserver.onCompleted();
    }

    private LoadShardReplicaResponse loadShardReplicaHandler(LoadShardReplicaMessage request) {
        int shardNum = request.getShard();
        assert(!dataStore.primaryShardMap.containsKey(shardNum));
        assert(!dataStore.replicaShardMap.containsKey(shardNum));
        // Get shard info from ZK.
        ZKShardDescription zkShardDescription = dataStore.zkCurator.getZKShardDescription(shardNum);
        String cloudName = zkShardDescription.cloudName;
        int replicaVersion = zkShardDescription.versionNumber;
        int primaryDSID = zkShardDescription.primaryDSID;
        // Download the shard.
        Optional<S> loadedShard = dataStore.downloadShardFromCloud(shardNum, cloudName, replicaVersion);
        if (loadedShard.isEmpty()) {
            logger.error("DS{} Shard load failed {}", dataStore.dsID, shardNum);
            return LoadShardReplicaResponse.newBuilder().setReturnCode(1).build();
        }
        // Load but lock the replica until it has been bootstrapped.
        S shard = loadedShard.get();
        dataStore.shardLockMap.put(shardNum, new ReentrantReadWriteLock());
        dataStore.shardLockMap.get(shardNum).writeLock().lock();
        dataStore.replicaShardMap.put(shardNum, loadedShard.get());

        // Set up a connection to the primary.
        DataStoreDescription primaryDSDescription = dataStore.zkCurator.getDSDescription(primaryDSID);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(primaryDSDescription.host, primaryDSDescription.port).usePlaintext().build();
        DataStoreDataStoreGrpc.DataStoreDataStoreBlockingStub primaryBlockingStub = DataStoreDataStoreGrpc.newBlockingStub(channel);

        // Bootstrap the replica, bringing it up to the same version as the primary.
        while (true) {  // TODO:  Stream the writes.
            BootstrapReplicaMessage m = BootstrapReplicaMessage.newBuilder()
                    .setShard(shardNum)
                    .setVersionNumber(replicaVersion)
                    .setDsID(dataStore.dsID)
                    .build();
            BootstrapReplicaResponse r;
            try {
                r = primaryBlockingStub.bootstrapReplica(m);
            } catch (StatusRuntimeException e) {
                channel.shutdown();
                shard.destroy();
                dataStore.replicaShardMap.remove(shardNum);
                dataStore.shardLockMap.get(shardNum).writeLock().unlock();
                dataStore.shardLockMap.remove(shardNum);
                logger.error("DS{} Replica Shard {} could not sync primary {}: {}", dataStore.dsID, shardNum, primaryDSID, e.getMessage());
                return LoadShardReplicaResponse.newBuilder().setReturnCode(1).build();
            }
            assert (r.getReturnCode() == 0);
            int primaryVersion = r.getVersionNumber();
            if (replicaVersion == primaryVersion) {
                // Loop until acknowledgement replica has caught up to primary.
                break;
            } else {
                // If not caught up, replay the primary's log.
                List<WriteQueryPlan<R, S>> writeQueryPlans = Arrays.asList((WriteQueryPlan<R, S>[]) Utilities.byteStringToObject(r.getWriteQueries()));
                List<R[]> rowsList = Arrays.asList((R[][]) Utilities.byteStringToObject(r.getWriteData()));
                assert(writeQueryPlans.size() == rowsList.size());
                for (int i = 0; i < writeQueryPlans.size(); i++) {
                    WriteQueryPlan<R, S> query = writeQueryPlans.get(i);
                    List<R> rows = Arrays.asList(rowsList.get(i));
                    assert(query.preCommit(shard, rows));
                    query.commit(shard);
                }
                replicaVersion = r.getVersionNumber();
            }
        }
        channel.shutdown();
        dataStore.writeLog.put(shardNum, new ConcurrentHashMap<>());
        dataStore.shardVersionMap.put(shardNum, replicaVersion);
        dataStore.shardLockMap.get(shardNum).writeLock().unlock();
        logger.info("DS{} Loaded new replica shard {} version {}", dataStore.dsID, shardNum, replicaVersion);
        return LoadShardReplicaResponse.newBuilder().setReturnCode(0).build();
    }

    @Override
    public void coordinatorPing(CoordinatorPingMessage request, StreamObserver<CoordinatorPingResponse> responseObserver) {
        responseObserver.onNext(CoordinatorPingResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
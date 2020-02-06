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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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
        dataStore.QPSMap.put(shardNum, new ConcurrentHashMap<>());
        dataStore.shardVersionMap.put(shardNum, 0);
        dataStore.lastUploadedVersionMap.put(shardNum, 0);
        dataStore.writeLog.put(shardNum, new ConcurrentHashMap<>());
        dataStore.replicaDescriptionsMap.put(shardNum, new ArrayList<>());
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
        dataStore.QPSMap.put(shardNum, new ConcurrentHashMap<>());
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

    @Override
    public void promoteReplicaShard(PromoteReplicaShardMessage request, StreamObserver<PromoteReplicaShardResponse> responseObserver) {
        responseObserver.onNext(promoteReplicaShardHandler(request));
        responseObserver.onCompleted();
    }

    private PromoteReplicaShardResponse promoteReplicaShardHandler(PromoteReplicaShardMessage message) {
        Integer shardNum = message.getShard();
        dataStore.shardLockMap.get(shardNum).writeLock().lock();
        assert(dataStore.replicaShardMap.containsKey(shardNum));
        assert(!dataStore.primaryShardMap.containsKey(shardNum));
        ZKShardDescription zkShardDescription = dataStore.zkCurator.getZKShardDescription(shardNum);
        dataStore.lastUploadedVersionMap.put(shardNum, zkShardDescription.versionNumber);
        assert(!dataStore.replicaDescriptionsMap.containsKey(shardNum));
        dataStore.replicaDescriptionsMap.put(shardNum, new ArrayList<>());
        Optional<List<DataStoreDescription>> replicaDescriptions = dataStore.zkCurator.getShardReplicaDSDescriptions(shardNum);
        if (replicaDescriptions.isPresent()) {
            for (DataStoreDescription dsDescription: replicaDescriptions.get()) {
                if (dsDescription.dsID != dataStore.dsID) {
                    ManagedChannel channel = ManagedChannelBuilder.forAddress(dsDescription.host, dsDescription.port).usePlaintext().build();
                    DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                    ReplicaDescription rd = new ReplicaDescription(dsDescription.dsID, channel, stub);
                    dataStore.replicaDescriptionsMap.get(shardNum).add(rd);
                }
            }
        }
        S shard = dataStore.replicaShardMap.get(shardNum);
        dataStore.replicaShardMap.remove(shardNum);
        dataStore.primaryShardMap.put(shardNum, shard);
        dataStore.shardLockMap.get(shardNum).writeLock().unlock();
        logger.info("DS{} promoted shard {} to primary", dataStore.dsID, shardNum);
        return PromoteReplicaShardResponse.newBuilder().build();
    }

    @Override
    public void removeShard(RemoveShardMessage request, StreamObserver<RemoveShardResponse> responseObserver) {
        responseObserver.onNext(removeShardHandler(request));
        responseObserver.onCompleted();
    }

    private RemoveShardResponse removeShardHandler(RemoveShardMessage message) {
        Integer shardNum = message.getShard();
        dataStore.shardLockMap.get(shardNum).writeLock().lock();
        assert(dataStore.replicaShardMap.containsKey(shardNum) || dataStore.primaryShardMap.containsKey(shardNum));
        S shard = dataStore.replicaShardMap.getOrDefault(shardNum, null);
        if (shard == null) {
            // Is primary.
            shard = dataStore.primaryShardMap.getOrDefault(shardNum, null);
            for (ManagedChannel channel: dataStore.replicaDescriptionsMap.get(shardNum).stream().map(i -> i.channel).collect(Collectors.toList())) {
                channel.shutdown();
            }
        } else {
            // Is replica.
            ZKShardDescription zkShardDescription = dataStore.zkCurator.getZKShardDescription(shardNum);
            DataStoreDescription primaryDSDescription = dataStore.zkCurator.getDSDescription(zkShardDescription.primaryDSID);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(primaryDSDescription.host, primaryDSDescription.port).usePlaintext().build();
            DataStoreDataStoreGrpc.DataStoreDataStoreBlockingStub stub = DataStoreDataStoreGrpc.newBlockingStub(channel);
            NotifyReplicaRemovedMessage m = NotifyReplicaRemovedMessage.newBuilder().setShard(shardNum).setDsID(dataStore.dsID).build();
            NotifyReplicaRemovedResponse r = stub.notifyReplicaRemoved(m);
            channel.shutdown();
        }
        shard.destroy();
        dataStore.primaryShardMap.remove(shardNum);
        dataStore.replicaShardMap.remove(shardNum);
        dataStore.writeLog.remove(shardNum);
        dataStore.replicaDescriptionsMap.remove(shardNum);
        dataStore.lastUploadedVersionMap.remove(shardNum);
        dataStore.shardVersionMap.remove(shardNum);
        dataStore.shardLockMap.get(shardNum).writeLock().unlock();
        logger.info("DS{} removed shard {}", dataStore.dsID, shardNum);
        return RemoveShardResponse.newBuilder().build();
    }

    @Override
    public void shardUsage(ShardUsageMessage request, StreamObserver<ShardUsageResponse> responseObserver) {
        responseObserver.onNext(shardUsageHandler(request));
        responseObserver.onCompleted();
    }

    private ShardUsageResponse shardUsageHandler(ShardUsageMessage message) {
        Map<Integer, Integer> shardQPS = new HashMap<>();
        long currentTime = Instant.now().getEpochSecond();
        for(Map.Entry<Integer, Map<Long, Integer>> entry: dataStore.QPSMap.entrySet()) {
            int shardNum = entry.getKey();
            int recentQPS = entry.getValue().entrySet().stream()
                    .filter(i -> i.getKey() > currentTime - DataStore.qpsReportTimeInterval)
                    .map(Map.Entry::getValue).mapToInt(i -> i).sum();
            shardQPS.put(shardNum, recentQPS);
        }
        Map<Integer, Integer> shardMemoryUsages = new HashMap<>();
        for(Map.Entry<Integer, S> entry: dataStore.primaryShardMap.entrySet()) {
            int shardNum = entry.getKey();
            int shardMemoryUsage = entry.getValue().getMemoryUsage();
            shardMemoryUsages.put(shardNum, shardMemoryUsage);
        }
        for(Map.Entry<Integer, S> entry: dataStore.replicaShardMap.entrySet()) {
            int shardNum = entry.getKey();
            int shardMemoryUsage = entry.getValue().getMemoryUsage();
            shardMemoryUsages.put(shardNum, shardMemoryUsage);
        }
        return ShardUsageResponse.newBuilder()
                .putAllShardQPS(shardQPS)
                .putAllShardMemoryUsage(shardMemoryUsages)
                .build();
    }
}
package edu.stanford.futuredata.uniserve.datastore;

import com.sun.management.OperatingSystemMXBean;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

class ServiceCoordinatorDataStore<R extends Row, S extends Shard> extends CoordinatorDataStoreGrpc.CoordinatorDataStoreImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceCoordinatorDataStore.class);
    private final DataStore<R, S> dataStore;

    ServiceCoordinatorDataStore(DataStore<R, S> dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void loadShardReplica(LoadShardReplicaMessage request, StreamObserver<LoadShardReplicaResponse> responseObserver) {
        responseObserver.onNext(loadShardReplicaHandler(request));
        responseObserver.onCompleted();
    }

    private LoadShardReplicaResponse loadShardReplicaHandler(LoadShardReplicaMessage request) {
        int returnCode = addReplica(request.getShard(), request.getIsReplacementPrimary());
        return LoadShardReplicaResponse.newBuilder().setReturnCode(returnCode).build();
    }

    private Integer addReplica(int shardNum, boolean isReplacementPrimary) {
        long loadStart = System.currentTimeMillis();
        assert(!dataStore.shardMap.containsKey(shardNum));
        // Get shard info from ZK.
        ZKShardDescription zkShardDescription = dataStore.zkCurator.getZKShardDescription(shardNum);
        assert (zkShardDescription != null);
        String cloudName = zkShardDescription.cloudName;
        int replicaVersion = zkShardDescription.versionNumber;
        // Download the shard.
        Optional<S> loadedShard = dataStore.downloadShardFromCloud(shardNum, cloudName, replicaVersion, true);
        if (loadedShard.isEmpty()) {
            logger.error("DS{} Shard load failed {}", dataStore.dsID, shardNum);
            return 1;
        }
        // Load but lock the replica until it has been bootstrapped.
        S shard = loadedShard.get();
        dataStore.createShardMetadata(shardNum);
        dataStore.shardMap.put(shardNum, shard);

        // Set up a connection to the primary.
        ConsistentHash c = dataStore.zkCurator.getConsistentHashFunction();
        int primaryDSID = c.getBuckets(shardNum).get(0);
        DataStoreDescription primaryDSDescription = dataStore.zkCurator.getDSDescription(primaryDSID);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(primaryDSDescription.host, primaryDSDescription.port).usePlaintext().build();
        DataStoreDataStoreGrpc.DataStoreDataStoreBlockingStub primaryBlockingStub = DataStoreDataStoreGrpc.newBlockingStub(channel);

        // Bootstrap the replica, bringing it up to the same version as the primary.
        while (!isReplacementPrimary) {  // TODO:  Stream the writes.
            BootstrapReplicaMessage m = BootstrapReplicaMessage.newBuilder()
                    .setShard(shardNum)
                    .setVersionNumber(replicaVersion)
                    .setDsID(dataStore.dsID)
                    .build();
            BootstrapReplicaResponse r;
            try {
                r = primaryBlockingStub.bootstrapReplica(m);
            } catch (StatusRuntimeException e) {
                logger.error("DS{} Replica Shard {} could not sync primary {}: {}", dataStore.dsID, shardNum, primaryDSID, e.getMessage());
                break;
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
        dataStore.shardVersionMap.put(shardNum, replicaVersion);
        if (isReplacementPrimary) {
            logger.info("DS{} Loaded replacement primary shard {} version {}. Load time: {}ms", dataStore.dsID, shardNum, replicaVersion, System.currentTimeMillis() - loadStart);
        } else {
            logger.info("DS{} Loaded new replica shard {} version {}. Load time: {}ms", dataStore.dsID, shardNum, replicaVersion, System.currentTimeMillis() - loadStart);
        }
        return 0;
    }

    @Override
    public void coordinatorPing(CoordinatorPingMessage request, StreamObserver<CoordinatorPingResponse> responseObserver) {
        responseObserver.onNext(CoordinatorPingResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void removeShard(RemoveShardMessage request, StreamObserver<RemoveShardResponse> responseObserver) {
        responseObserver.onNext(removeShardHandler(request));
        responseObserver.onCompleted();
    }

    private RemoveShardResponse removeShardHandler(RemoveShardMessage message) {
        removeShard(message.getShard());
        return RemoveShardResponse.newBuilder().build();
    }

    private void removeShard(int shardNum) {
        dataStore.shardLockMap.get(shardNum).systemLockLock();
        assert(dataStore.shardMap.containsKey(shardNum));
        S shard = dataStore.shardMap.getOrDefault(shardNum, null);
        if (shard == null) {
            // Is primary.
            shard = dataStore.shardMap.getOrDefault(shardNum, null);
            for (ManagedChannel channel: dataStore.replicaDescriptionsMap.get(shardNum).stream().map(i -> i.channel).collect(Collectors.toList())) {
                channel.shutdown();
            }
        }
        shard.destroy();
        dataStore.shardMap.remove(shardNum);
        dataStore.writeLog.get(shardNum).clear();
        dataStore.replicaDescriptionsMap.get(shardNum).forEach(i -> i.channel.shutdown());
        dataStore.replicaDescriptionsMap.get(shardNum).clear();
        dataStore.shardVersionMap.remove(shardNum);
        dataStore.shardLockMap.get(shardNum).systemLockUnlock();
        logger.info("DS{} removed shard {}", dataStore.dsID, shardNum);
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
        for(Map.Entry<Integer, S> entry: dataStore.shardMap.entrySet()) {
            int shardNum = entry.getKey();
            int shardMemoryUsage = entry.getValue().getMemoryUsage();
            shardMemoryUsages.put(shardNum, shardMemoryUsage);
        }
        OperatingSystemMXBean bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return ShardUsageResponse.newBuilder()
                .setDsID(dataStore.dsID)
                .putAllShardQPS(shardQPS)
                .putAllShardMemoryUsage(shardMemoryUsages)
                .setServerCPUUsage(bean.getSystemCpuLoad())
                .build();
    }

    @Override
    public void notifyReplicaRemoved(NotifyReplicaRemovedMessage request, StreamObserver<NotifyReplicaRemovedResponse> responseObserver) {
        responseObserver.onNext(notifyReplicaRemovedHandler(request));
        responseObserver.onCompleted();
    }

    private NotifyReplicaRemovedResponse notifyReplicaRemovedHandler(NotifyReplicaRemovedMessage request) {
        int shardNum = request.getShard();
        int dsID = request.getDsID();
        dataStore.shardLockMap.get(shardNum).writerLockLock();
        List<ReplicaDescription> shardReplicaDescriptions = dataStore.replicaDescriptionsMap.get(shardNum);
        List<ReplicaDescription> matchingDescriptions = shardReplicaDescriptions.stream().filter(i -> i.dsID == dsID).collect(Collectors.toList());
        if (matchingDescriptions.size() > 0) {
            assert(matchingDescriptions.size() == 1);
            matchingDescriptions.get(0).channel.shutdown();
            shardReplicaDescriptions.remove(matchingDescriptions.get(0));
            logger.info("DS{} removed replica of shard {} on DS{}", dataStore.dsID, shardNum, dsID);
        } else {
            logger.info("DS{} removed UNKNOWN replica shard {} on DS{}", dataStore.dsID, shardNum, dsID);
        }
        dataStore.shardLockMap.get(shardNum).writerLockUnlock();
        return NotifyReplicaRemovedResponse.newBuilder().build();
    }

    private ConsistentHash oldHash;

    @Override
    public void executeReshuffleAdd(ExecuteReshuffleMessage m, StreamObserver<ExecuteReshuffleResponse> responseObserver) {
        ConsistentHash newHash = (ConsistentHash) Utilities.byteStringToObject(m.getNewConsistentHash());
        ConsistentHash oldHash = dataStore.consistentHash;
        this.oldHash = oldHash;
        dataStore.dsID = m.getDsID();
        for (int shardNum: m.getShardListList()) {
            if (newHash.getBuckets(shardNum).contains(m.getDsID()) && (oldHash == null || !oldHash.getBuckets(shardNum).contains(m.getDsID()))) {
                addReplica(shardNum, false);
            }
        }
        // By setting the consistent hash, ensure no new queries are processed after this point.
        dataStore.consistentHash = newHash;
        responseObserver.onNext(ExecuteReshuffleResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void executeReshuffleRemove(ExecuteReshuffleMessage m, StreamObserver<ExecuteReshuffleResponse> responseObserver) {
        ConsistentHash newHash = dataStore.consistentHash;
        ConsistentHash oldHash = this.oldHash;
        // Delete all shards to be shuffled out, if present.
        for (int shardNum: dataStore.shardLockMap.keySet()) {
            if (oldHash != null && oldHash.getBuckets(shardNum).contains(m.getDsID()) && !newHash.getBuckets(shardNum).contains(m.getDsID())) {
                removeShard(shardNum);
            }
        }
        // After this returns, no more queries can be executed on shards to be shuffled off this server.
        responseObserver.onNext(ExecuteReshuffleResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
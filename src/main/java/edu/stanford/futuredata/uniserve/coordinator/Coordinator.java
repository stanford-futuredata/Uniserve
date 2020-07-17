package edu.stanford.futuredata.uniserve.coordinator;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Coordinator {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final String coordinatorHost;
    private final int coordinatorPort;
    private final Server server;
    final CoordinatorCurator zkCurator;

    private final CoordinatorCloud cCloud;

    final Lock consistentHashLock = new ReentrantLock();
    public final ConsistentHash consistentHash = new ConsistentHash();

    // Used to assign each datastore a unique incremental ID.
    final AtomicInteger dataStoreNumber = new AtomicInteger(0);
    // Map from datastore IDs to their descriptions.
    final Map<Integer, DataStoreDescription> dataStoresMap = new ConcurrentHashMap<>();
    // Used to assign each table a unique incremental ID.
    final AtomicInteger tableNumber = new AtomicInteger(0);
    // Map from table names to IDs.
    final Map<String, Integer> tableIDMap = new ConcurrentHashMap<>();
    // Maximum number of shards in each table.
    final Map<String, Integer> tableNumShardsMap = new ConcurrentHashMap<>();
    // Map from datastore IDs to their channels.
    final Map<Integer, ManagedChannel> dataStoreChannelsMap = new ConcurrentHashMap<>();
    // Map from datastore IDs to their stubs.
    final Map<Integer, CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub> dataStoreStubsMap = new ConcurrentHashMap<>();
    // Map from shards to their replicas.
    final Map<Integer, List<Integer>> shardToReplicaDataStoreMap = new ConcurrentHashMap<>();
    // Map from sets of shards touched by query to frequency.
    public Map<Set<Integer>, Integer> queryStatistics = new ConcurrentHashMap<>();
    // Lock on the queryStatistics map.
    final Lock statisticsLock = new ReentrantLock();
    // Maps from DSIDs to the cloud IDs uniquely assigned by the autoscaler to new datastores.
    public Map<Integer, Integer> dsIDToCloudID = new ConcurrentHashMap<>();

    public boolean runLoadBalancerDaemon = true;
    private final LoadBalancerDaemon loadBalancer;
    public static int loadBalancerSleepDurationMillis = 60000;
    public final Semaphore loadBalancerSemaphore = new Semaphore(0);

    // Lock protects shardToPrimaryDataStoreMap, shardToReplicaDataStoreMap, and shardToReplicaRatioMap.
    // Each operation modifying these maps follows this process:  Lock, change the local copies, unlock, perform
    // the operation, lock, retrieve the local copies, set the ZK mirrors to the local copies, unlock.
    public final Lock shardMapLock = new ReentrantLock();

    public Coordinator(CoordinatorCloud cCloud, String zkHost, int zkPort, String coordinatorHost, int coordinatorPort) {
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        zkCurator = new CoordinatorCurator(zkHost, zkPort);
        this.server = ServerBuilder.forPort(coordinatorPort)
                .addService(new ServiceDataStoreCoordinator(this))
                .addService(new ServiceBrokerCoordinator(this))
                .build();
        loadBalancer = new LoadBalancerDaemon();
        this.cCloud = cCloud;
    }

    /** Start serving requests. */
    public int startServing() {
        try {
            server.start();
            zkCurator.registerCoordinator(coordinatorHost, coordinatorPort);
        } catch (Exception e) {
            logger.warn("Coordinator startup failed: {}", e.getMessage());
            this.stopServing();
            return 1;
        }
        logger.info("Coordinator server started, listening on " + coordinatorPort);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Coordinator.this.stopServing();
            }
        });
        loadBalancer.start();
        return 0;
    }

    /** Stop serving requests and shutdown resources. */
    public void stopServing() {
        if (server != null) {
            server.shutdown();
        }
        for(ManagedChannel channel: dataStoreChannelsMap.values()) {
            channel.shutdown();
        }
        runLoadBalancerDaemon = false;
        try {
            loadBalancer.interrupt();
            loadBalancer.join();
        } catch (InterruptedException ignored) {}
        zkCurator.close();
        if (cCloud != null) {
            cCloud.shutdown();
        }
    }

    public void addReplica(int shardNum, int replicaID) {
        shardMapLock.lock();
        if (dataStoresMap.get(replicaID).status.get() == DataStoreDescription.DEAD) {
            shardMapLock.unlock();
            logger.info("AddReplica failed for shard {} DEAD DataStore {}", shardNum, replicaID);
            return;
        }
        int primaryDataStore = consistentHash.getBucket(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.putIfAbsent(shardNum, new ArrayList<>());
        if (replicaDataStores == null) {
            replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        }
        if (replicaDataStores.contains(replicaID)) {
            // Replica already exists.
            shardMapLock.unlock();
        } else if (replicaID != primaryDataStore) {
            replicaDataStores.add(replicaID);
            shardMapLock.unlock();
            // Create new replica.
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(replicaID);
            LoadShardReplicaMessage m = LoadShardReplicaMessage.newBuilder().setShard(shardNum).setIsReplacementPrimary(false).build();
            try {
                LoadShardReplicaResponse r = stub.loadShardReplica(m);
                if (r.getReturnCode() != 0) {  // TODO: Might lead to unavailable shard.
                    logger.warn("Shard {} load failed on DataStore {}", shardNum, replicaID);
                    shardMapLock.lock();
                    replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
                    if (replicaDataStores.contains(replicaID)) {
                        replicaDataStores.remove(Integer.valueOf(replicaID));
                    }
                    shardMapLock.unlock();
                    return;
                }
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} load RPC failed on DataStore {}", shardNum, replicaID);
            }
        } else {
            shardMapLock.unlock();
            logger.info("AddReplica failed for shard {} PRIMARY DataStore {}", shardNum, replicaID);
        }
    }

    public void removeShard(int shardNum, int targetID) {
        shardMapLock.lock();
        int primaryDataStore = consistentHash.getBucket(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        if (primaryDataStore == targetID) {
            logger.error("Cannot remove shard {} from primary {}", shardNum, targetID);
        } else {
            if (!replicaDataStores.contains(targetID)) {
                logger.info("RemoveShard Failed DataStore {} does not have shard {}", targetID, shardNum);
                shardMapLock.unlock();
                return;
            }
            int targetIndex = replicaDataStores.indexOf(targetID);
            replicaDataStores.remove(Integer.valueOf(targetID));
            shardMapLock.unlock();
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(targetID);
            RemoveShardMessage m = RemoveShardMessage.newBuilder().setShard(shardNum).build();
            try {
                RemoveShardResponse r = stub.removeShard(m);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} remove RPC failed on DataStore {}", shardNum, targetID);
            }
            shardMapLock.lock();
            primaryDataStore = consistentHash.getBucket(shardNum);
            NotifyReplicaRemovedMessage nm = NotifyReplicaRemovedMessage.newBuilder().setDsID(targetID).setShard(shardNum).build();
            try {
                NotifyReplicaRemovedResponse r = dataStoreStubsMap.get(primaryDataStore).notifyReplicaRemoved(nm);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} removal notification RPC for DataStore {} failed on primary DataStore {}", shardNum, targetID, primaryDataStore);
            }
            shardMapLock.unlock();
        }
    }

    /** Construct three load maps:
     * 1.  Shard number to shard QPS.
     * 2.  Shard number to shard memory usage.
     * 3.  DSID to datastore CPU usage.
     * **/
    public Triplet<Map<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Double>> collectLoad() {
        Map<Integer, Integer> qpsMap = new ConcurrentHashMap<>();
        Map<Integer, Integer> memoryUsagesMap = new ConcurrentHashMap<>();
        Map<Integer, Integer> shardCountMap = new ConcurrentHashMap<>();
        Map<Integer, Double> serverCpuUsageMap = new ConcurrentHashMap<>();
        for(DataStoreDescription dsDesc: dataStoresMap.values()) {
            if (dsDesc.status.get() == DataStoreDescription.ALIVE) {
                CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(dsDesc.dsID);
                ShardUsageMessage m = ShardUsageMessage.newBuilder().build();
                try {
                    ShardUsageResponse response = stub.shardUsage(m);
                    Map<Integer, Integer> dataStoreQPSMap = response.getShardQPSMap();
                    Map<Integer, Integer> dataStoreUsageMap = response.getShardMemoryUsageMap();
                    dataStoreQPSMap.forEach((key, value) -> qpsMap.merge(key, value, Integer::sum));
                    dataStoreUsageMap.forEach((key, value) -> memoryUsagesMap.merge(key, value, Integer::sum));
                    dataStoreUsageMap.forEach((key, value) -> shardCountMap.merge(key, 1, (v1, v2) -> v1 + 1));
                    serverCpuUsageMap.put(response.getDsID(), response.getServerCPUUsage());
                } catch (StatusRuntimeException e) {
                    logger.info("DS{} load collection failed: {}", dsDesc.dsID, e.getMessage());
                }
            }
        }
        memoryUsagesMap.replaceAll((k, v) -> shardCountMap.get(k) > 0 ? v / shardCountMap.get(k) : v);
        return new Triplet<>(qpsMap, memoryUsagesMap, serverCpuUsageMap);
    }

    private int quiescence = 0;
    public final int quiescencePeriod = 2;
    public final double addServerThreshold = 0.7;
    public final double removeServerThreshold = 0.3;

    public void autoScale(Map<Integer, Double> serverCpuUsage) {
        OptionalDouble averageCpuUsageOpt = serverCpuUsage.values().stream().mapToDouble(i -> i).average();
        if (averageCpuUsageOpt.isEmpty()) {
            return;
        }
        double averageCpuUsage = averageCpuUsageOpt.getAsDouble();
        logger.info("Average CPU Usage: {}", averageCpuUsage);
        // After acting, wait quiescencePeriod cycles before acting again for CPU to rebalance.
        if (quiescence > 0) {
            quiescence--;
            return;
        }
        // Add a server.
        if (averageCpuUsage > addServerThreshold) {
            logger.info("Adding DataStore");
            boolean success = cCloud.addDataStore();
            if (!success) {
                logger.info("DataStore addition failed");
            }
            quiescence = quiescencePeriod;
        }
        // Remove a server.
        if (averageCpuUsage < removeServerThreshold) {
            List<Integer> removeableDSIDs = dataStoresMap.keySet().stream()
                    .filter(i -> dataStoresMap.get(i).status.get() == DataStoreDescription.ALIVE)
                    .filter(i -> dsIDToCloudID.containsKey(i))
                    .collect(Collectors.toList());
            if (removeableDSIDs.size() > 0) {
                // TODO:  Remove a datastore.
            }
        }
    }

    public void assignShards(Set<Integer> lostShards, Set<Integer> gainedShards) {
        ByteString newConsistentHash = Utilities.objectToByteString(consistentHash);
        ExecuteReshuffleMessage reshuffleMessage = ExecuteReshuffleMessage.newBuilder()
                .setNewConsistentHash(newConsistentHash).build();
        // TODO:  Parallelize
        for (int dsID: lostShards) {
            dataStoreStubsMap.get(dsID).executeReshuffle(reshuffleMessage);
        }
        for (int dsID: gainedShards) {
            dataStoreStubsMap.get(dsID).executeReshuffle(reshuffleMessage);
        }
        zkCurator.setConsistentHashFunction(consistentHash);
    }

    private class LoadBalancerDaemon extends Thread {
        @Override
        public void run() {
            while (runLoadBalancerDaemon) {
                try {
                    loadBalancerSemaphore.tryAcquire(loadBalancerSleepDurationMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    return;
                }
                Triplet<Map<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Double>> load = collectLoad();
                Map<Integer, Integer> qpsLoad = load.getValue0();
                Map<Integer, Integer> memoryUsages = load.getValue1();
                logger.info("Collected QPS Load: {}", qpsLoad);
                logger.info("Collected memory usages: {}", memoryUsages);
                consistentHashLock.lock();
                Pair<Set<Integer>, Set<Integer>> changes = LoadBalancer.balanceLoad(qpsLoad, consistentHash);
                Set<Integer> lostShards = changes.getValue0();
                Set<Integer> gainedShards = changes.getValue1();
                logger.info("Lost shards: {}  Gained shards: {}", lostShards, gainedShards);
                assignShards(lostShards, gainedShards);
                consistentHashLock.unlock();
                Map<Integer, Double> serverCpuUsage = load.getValue2();
                logger.info("Collected DataStore CPU Usage: {}", serverCpuUsage);
                if (cCloud != null) {
                    autoScale(serverCpuUsage);
                }
            }
        }
    }
}

package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import ilog.concert.IloException;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Coordinator {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final String coordinatorHost;
    private final int coordinatorPort;
    private final Server server;
    final CoordinatorCurator zkCurator;

    // Used to assign each datastore a unique incremental ID.
    final AtomicInteger dataStoreNumber = new AtomicInteger(0);
    // Map from datastore IDs to their descriptions.
    final Map<Integer, DataStoreDescription> dataStoresMap = new ConcurrentHashMap<>();
    // Map from datastore IDs to their channels.
    final Map<Integer, ManagedChannel> dataStoreChannelsMap = new ConcurrentHashMap<>();
    // Map from datastore IDs to their stubs.
    final Map<Integer, CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub> dataStoreStubsMap = new ConcurrentHashMap<>();
    // Map from shards to last uploaded versions.
    final Map<Integer, Integer> shardToVersionMap = new ConcurrentHashMap<>();
    // Map from shards to their primaries.
    final Map<Integer, Integer> shardToPrimaryDataStoreMap = new ConcurrentHashMap<>();
    // Map from shards to their replicas.
    final Map<Integer, List<Integer>> shardToReplicaDataStoreMap = new ConcurrentHashMap<>();
    // Map from shards to their replicas' ratios.
    final Map<Integer, List<Double>> shardToReplicaRatioMap = new ConcurrentHashMap<>();

    public boolean runLoadBalancerDaemon = true;
    private final LoadBalancerDaemon loadBalancer;
    public final static int loadBalancerSleepDurationMillis = 60000;

    // Protects the shard maps and ensures their local and ZK versions are kept in sync between functions.
    public final Lock shardMapLock = new ReentrantLock();

    public Coordinator(String zkHost, int zkPort, String coordinatorHost, int coordinatorPort) {
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        zkCurator = new CoordinatorCurator(zkHost, zkPort);
        this.server = ServerBuilder.forPort(coordinatorPort)
                .addService(new ServiceDataStoreCoordinator(this))
                .addService(new ServiceBrokerCoordinator(this))
                .build();
        loadBalancer = new LoadBalancerDaemon();
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
    }

    public boolean addReplica(int shardNum, int replicaID, double ratio) {
        shardMapLock.lock();
        int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        List<Double> replicaRatios = shardToReplicaRatioMap.get(shardNum);
        assert (replicaID != primaryDataStore);
        assert(replicaRatios.size() == replicaDataStores.size());
        if (replicaDataStores.contains(replicaID)) {
            // Update existing replica with new ratio.
            int replicaIndex = replicaDataStores.indexOf(replicaID);
            replicaRatios.set(replicaIndex, ratio);
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            zkCurator.setZKShardDescription(shardNum, primaryDataStore, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
        } else {
            // Create new replica.
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(replicaID);
            LoadShardReplicaMessage m = LoadShardReplicaMessage.newBuilder().setShard(shardNum).build();
            try {
                LoadShardReplicaResponse r = stub.loadShardReplica(m);
                if (r.getReturnCode() != 0) {
                    logger.warn("Shard {} load failed on DataStore {}", shardNum, replicaID);
                    shardMapLock.unlock();
                    return false;
                }
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} load RPC failed on DataStore {}", shardNum, replicaID);
                shardMapLock.unlock();
                return false;
            }
            replicaDataStores.add(replicaID);
            replicaRatios.add(ratio);
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            zkCurator.setZKShardDescription(shardNum, primaryDataStore, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
        }
        shardMapLock.unlock();
        return true;
    }

    public void removeShard(int shardNum, int targetID) {
        shardMapLock.lock();
        int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        List<Double> replicaRatios = shardToReplicaRatioMap.get(shardNum);
        if (primaryDataStore == targetID) {
            assert(replicaDataStores.size() > 0);
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub primaryStub = dataStoreStubsMap.get(targetID);
            RemoveShardMessage removeShardMessage = RemoveShardMessage.newBuilder().setShard(shardNum).build();
            RemoveShardResponse removeShardResponse = primaryStub.removeShard(removeShardMessage);
            Integer newPrimary = replicaDataStores.get(ThreadLocalRandom.current().nextInt(0, replicaDataStores.size()));
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub newPrimaryStub = dataStoreStubsMap.get(newPrimary);
            PromoteReplicaShardMessage promoteReplicaShardMessage =
                    PromoteReplicaShardMessage.newBuilder().setShard(shardNum).build();
            PromoteReplicaShardResponse promoteReplicaShardResponse =
                    newPrimaryStub.promoteReplicaShard(promoteReplicaShardMessage);
            int newPrimaryIndex = replicaDataStores.indexOf(newPrimary);
            replicaDataStores.remove(newPrimary);
            replicaRatios.remove(newPrimaryIndex);
            shardToPrimaryDataStoreMap.put(shardNum, newPrimary);
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            zkCurator.setZKShardDescription(shardNum, newPrimary, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
        } else {
            assert(replicaDataStores.contains(targetID));
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(targetID);
            RemoveShardMessage m = RemoveShardMessage.newBuilder().setShard(shardNum).build();
            try {
                RemoveShardResponse r = stub.removeShard(m);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} remove RPC failed on DataStore {}", shardNum, targetID);
                shardMapLock.unlock();
                assert(false);
            }
            int targetIndex = replicaDataStores.indexOf(targetID);
            replicaDataStores.remove(Integer.valueOf(targetID));
            replicaRatios.remove(targetIndex);
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            zkCurator.setZKShardDescription(shardNum, primaryDataStore, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
        }
        shardMapLock.unlock();
    }

    /** Construct a map from shard number to the shard's QPS and memory usage **/
    public Pair<Map<Integer, Integer>, Map<Integer, Integer>> collectLoad() {
        Map<Integer, Integer> qpsMap = new ConcurrentHashMap<>();
        Map<Integer, Integer> memoryUsagesMap = new ConcurrentHashMap<>();
        Map<Integer, Integer> shardCountMap = new ConcurrentHashMap<>();
        List<Thread> threads = new ArrayList<>();
        for(CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub: dataStoreStubsMap.values()) {
            Runnable r = () -> {
                ShardUsageMessage m = ShardUsageMessage.newBuilder().build();
                ShardUsageResponse response = stub.shardUsage(m);
                Map<Integer, Integer> dataStoreQPSMap = response.getShardQPSMap();
                Map<Integer, Integer> dataStoreUsageMap = response.getShardMemoryUsageMap();
                dataStoreQPSMap.forEach((key, value) -> qpsMap.merge(key, value, Integer::sum));
                dataStoreUsageMap.forEach((key, value) -> memoryUsagesMap.merge(key, value, Integer::sum));
                dataStoreUsageMap.forEach((key, value) -> shardCountMap.merge(key, 1, (v1, v2) -> v1 + 1));
            };
            Thread t = new Thread(r);
            t.start();
            threads.add(t);
        }
        for (Thread thread: threads) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {}
        }
        memoryUsagesMap.replaceAll((k, v) -> v / shardCountMap.get(k));
        return new Pair<>(qpsMap, memoryUsagesMap);
    }

    /** Take in maps from shards to loads, return a map from DSIDs to shards assigned to that datastore and their ratios. **/
    public Map<Integer, Map<Integer, Double>> getShardAssignments(Map<Integer, Integer> qpsLoad, Map<Integer, Integer> memoryLoad) {
        int numShards = shardToPrimaryDataStoreMap.size();
        int numServers = dataStoresMap.size();
        Map<Integer, Integer> indexToDSIDMap = new HashMap<>();
        Map<Integer, Integer> dsIDToIndexMap = new HashMap<>();
        Map<Integer, Integer> indexToShardNumMap = new HashMap<>();
        Map<Integer, Integer> shardNumToIndexMap = new HashMap<>();
        int[] shardLoads = new int[numShards];
        int[] shardMemoryUsages = new int[numShards];
        int[][] currentLocations = new int[numServers][];

        int shardIndex = 0;
        for(int shardNum: qpsLoad.keySet()) {
            indexToShardNumMap.put(shardIndex, shardNum);
            shardNumToIndexMap.put(shardNum, shardIndex);
            shardLoads[shardIndex] = qpsLoad.get(shardNum);
            shardMemoryUsages[shardIndex] = memoryLoad.get(shardNum);
            shardIndex++;
        }
        int serverIndex = 0;
        for(int dsID: dataStoresMap.keySet()) {
            indexToDSIDMap.put(serverIndex, dsID);
            dsIDToIndexMap.put(dsID, serverIndex);
            currentLocations[serverIndex] = new int[numShards];
            serverIndex++;
        }
        for(Map.Entry<Integer, Integer> entry: shardToPrimaryDataStoreMap.entrySet()) {
            int shardNum = entry.getKey();
            int dsID = entry.getValue();
            currentLocations[dsIDToIndexMap.get(dsID)][shardNumToIndexMap.get(shardNum)] = 1;
        }
        for(Map.Entry<Integer, List<Integer>> entry: shardToReplicaDataStoreMap.entrySet()) {
            int shardNum = entry.getKey();
            for(int dsID: entry.getValue()) {
                currentLocations[dsIDToIndexMap.get(dsID)][shardNumToIndexMap.get(shardNum)] = 1;
            }
        }
        List<double[]> serverShardRatios = null;
        try {
            int maxMemory = 1000000;  // TODO:  Actually set this.
            serverShardRatios = LoadBalancer.balanceLoad(numShards, numServers, shardLoads, shardMemoryUsages, currentLocations, maxMemory);
        } catch (IloException e) {
            assert (false);
        }
        assert(serverShardRatios.size() == numServers);
        Map<Integer, Map<Integer, Double>> assignmentMap = new HashMap<>();
        for(int i = 0; i < numServers; i++) {
            int dsID = indexToDSIDMap.get(i);
            Map<Integer, Double> datastoreAssignmentMap = new HashMap<>();
            double[] shardRatios = serverShardRatios.get(i);
            assert(shardRatios.length == numShards);
            for(int j = 0; j < numShards; j++) {
                int shardNum = indexToShardNumMap.get(j);
                datastoreAssignmentMap.put(shardNum, shardRatios[j]);
            }
            assignmentMap.put(dsID, datastoreAssignmentMap);
        }
        return assignmentMap;
    }

    /** Use an assignmentMap to assign shards to datastores **/
    public void assignShards(Map<Integer, Map<Integer, Double>> assignmentMap) {
        for(Map.Entry<Integer, Map<Integer, Double>> entry: assignmentMap.entrySet()) {
            int dsID = entry.getKey();
            for(Map.Entry<Integer, Double> assignment: entry.getValue().entrySet()) {
                int shardNum = assignment.getKey();
                double shardRatio = assignment.getValue();
                if (shardRatio == 0.0) {
                    if (shardToPrimaryDataStoreMap.get(shardNum) == dsID) {
                        removeShard(shardNum, dsID);
                    }
                    if (shardToReplicaDataStoreMap.get(shardNum).contains(dsID)) {
                        removeShard(shardNum, dsID);
                    }
                } else {
                    if (shardToPrimaryDataStoreMap.get(shardNum) != dsID) {
                        addReplica(shardNum, dsID, shardRatio);
                    }
                }
            }
        }
    }

    private class LoadBalancerDaemon extends Thread {
        @Override
        public void run() {
            while (runLoadBalancerDaemon) {
                try {
                    Thread.sleep(loadBalancerSleepDurationMillis);
                } catch (InterruptedException e) {
                    return;
                }
                Pair<Map<Integer, Integer>, Map<Integer, Integer>> load = collectLoad();
                Map<Integer, Integer> qpsLoad = load.getValue0();
                Map<Integer, Integer> memoryUsages = load.getValue1();
                logger.info("Collected QPS Load: {}", qpsLoad);
                logger.info("Collected memory usages: {}", memoryUsages);
                Map<Integer, Map<Integer, Double>> assignmentMap = getShardAssignments(qpsLoad, memoryUsages);
                logger.info("Generated assignment map: {}", assignmentMap);
                assignShards(assignmentMap);
            }
        }
    }
}

package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import ilog.concert.IloException;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
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

    private static final LoadBalancer lb = new LoadBalancer();
    private final CoordinatorCloud cCloud;

    // Used to assign each datastore a unique incremental ID.
    final AtomicInteger dataStoreNumber = new AtomicInteger(0);
    // Map from datastore IDs to their descriptions.
    final Map<Integer, DataStoreDescription> dataStoresMap = new ConcurrentHashMap<>();
    // Map from datastore IDs to their channels.
    final Map<Integer, ManagedChannel> dataStoreChannelsMap = new ConcurrentHashMap<>();
    // Map from datastore IDs to their stubs.
    final Map<Integer, CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub> dataStoreStubsMap = new ConcurrentHashMap<>();
    // Map from shards to their primaries.
    final Map<Integer, Integer> shardToPrimaryDataStoreMap = new ConcurrentHashMap<>();
    // Map from shards to their replicas.
    final Map<Integer, List<Integer>> shardToReplicaDataStoreMap = new ConcurrentHashMap<>();
    // Map from shards to their replicas' ratios.
    final Map<Integer, List<Double>> shardToReplicaRatioMap = new ConcurrentHashMap<>();
    // Map from sets of shards touched by query to frequency.
    public Map<Set<Integer>, Integer> queryStatistics = new ConcurrentHashMap<>();
    // Lock on the queryStatistics map.
    final Lock statisticsLock = new ReentrantLock();
    // Maps from DSIDs to the cloud IDs uniquely assigned by the autoscaler to new datastores.
    public Map<Integer, Integer> dsIDToCloudID = new ConcurrentHashMap<>();

    public boolean runLoadBalancerDaemon = true;
    private final LoadBalancerDaemon loadBalancer;
    public static int loadBalancerSleepDurationMillis = 60000;

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
    }

    int assignShardToDataStore(int shardNum) {
        DataStoreDescription ds;
        int offset = 0;
        do {
            ds = dataStoresMap.get(shardNum % dataStoresMap.size() + offset);
            offset++;
        } while (ds.status.get() == DataStoreDescription.DEAD);
        return ds.dsID;
    }

    public void addReplica(int shardNum, int replicaID, double ratio) {
        shardMapLock.lock();
        if (dataStoresMap.get(replicaID).status.get() == DataStoreDescription.DEAD) {
            shardMapLock.unlock();
            logger.info("AddReplica failed for shard {} DEAD DataStore {}", shardNum, replicaID);
            return;
        }
        int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        List<Double> replicaRatios = shardToReplicaRatioMap.get(shardNum);
        assert(replicaRatios.size() == replicaDataStores.size());
        if (replicaDataStores.contains(replicaID)) {
            // Update existing replica with new ratio.
            replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
            assert(replicaRatios.size() == replicaDataStores.size());
            int replicaIndex = replicaDataStores.indexOf(replicaID);
            replicaRatios.set(replicaIndex, ratio);
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            zkCurator.setZKShardDescription(shardNum, primaryDataStore, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
            shardMapLock.unlock();
        } else if (replicaID != primaryDataStore) {
            replicaDataStores.add(replicaID);
            replicaRatios.add(ratio);
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
                    replicaRatios = shardToReplicaRatioMap.get(shardNum);
                    if (replicaDataStores.contains(replicaID)) {
                        int targetIndex = replicaDataStores.indexOf(replicaID);
                        replicaDataStores.remove(Integer.valueOf(replicaID));
                        replicaRatios.remove(targetIndex);
                        primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
                        ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
                        zkCurator.setZKShardDescription(shardNum, primaryDataStore, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
                    }
                    shardMapLock.unlock();
                    return;
                }
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} load RPC failed on DataStore {}", shardNum, replicaID);
            }
            shardMapLock.lock();
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
            replicaRatios = shardToReplicaRatioMap.get(shardNum);
            primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
            zkCurator.setZKShardDescription(shardNum, primaryDataStore, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
            shardMapLock.unlock();
        } else {
            shardMapLock.unlock();
            logger.info("AddReplica failed for shard {} PRIMARY DataStore {}", shardNum, replicaID);
        }
    }

    public void removeShard(int shardNum, int targetID) {
        shardMapLock.lock();
        int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        List<Double> replicaRatios = shardToReplicaRatioMap.get(shardNum);
        if (primaryDataStore == targetID) {
            Integer newPrimary;
            boolean replicasExist = replicaDataStores.size() > 0;
            if (replicasExist) {
                newPrimary = replicaDataStores.get(ThreadLocalRandom.current().nextInt(0, replicaDataStores.size()));
                int newPrimaryIndex = replicaDataStores.indexOf(newPrimary);
                replicaDataStores.remove(newPrimary);
                replicaRatios.remove(newPrimaryIndex);
            } else {
                newPrimary = assignShardToDataStore(shardNum);
            }
            shardToPrimaryDataStoreMap.put(shardNum, newPrimary);
            shardMapLock.unlock();
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub primaryStub = dataStoreStubsMap.get(targetID);
            RemoveShardMessage removeShardMessage = RemoveShardMessage.newBuilder().setShard(shardNum).build();
            try {
                RemoveShardResponse removeShardResponse = primaryStub.removeShard(removeShardMessage);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} remove RPC failed on DataStore {}", shardNum, targetID);
            }
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub newPrimaryStub = dataStoreStubsMap.get(newPrimary);
            if (replicasExist) {
                PromoteReplicaShardMessage promoteReplicaShardMessage =
                        PromoteReplicaShardMessage.newBuilder().setShard(shardNum).build();
                try {
                    PromoteReplicaShardResponse promoteReplicaShardResponse =
                            newPrimaryStub.promoteReplicaShard(promoteReplicaShardMessage);
                } catch (StatusRuntimeException e) {
                    logger.warn("Shard {} promote RPC failed on DataStore {}", shardNum, newPrimary);
                }
            } else {
                LoadShardReplicaMessage m = LoadShardReplicaMessage.newBuilder().setShard(shardNum).setIsReplacementPrimary(true).build();
                try {
                    LoadShardReplicaResponse r = newPrimaryStub.loadShardReplica(m);
                    if (r.getReturnCode() != 0) {
                        assert(false);
                        logger.warn("Shard {} load failed on DataStore {}", shardNum, newPrimary);
                    }
                } catch (StatusRuntimeException e) {
                    logger.warn("Shard {} load RPC failed on DataStore {}", shardNum, newPrimary);
                }
            }
            shardMapLock.lock();
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
            replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
            replicaRatios = shardToReplicaRatioMap.get(shardNum);
            zkCurator.setZKShardDescription(shardNum, primaryDataStore, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
            shardMapLock.unlock();
        } else {
            if (!replicaDataStores.contains(targetID)) {
                logger.info("RemoveShard Failed DataStore {} does not have shard {}", targetID, shardNum);
                shardMapLock.unlock();
                return;
            }
            int targetIndex = replicaDataStores.indexOf(targetID);
            replicaDataStores.remove(Integer.valueOf(targetID));
            replicaRatios.remove(targetIndex);
            shardMapLock.unlock();
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(targetID);
            RemoveShardMessage m = RemoveShardMessage.newBuilder().setShard(shardNum).build();
            try {
                RemoveShardResponse r = stub.removeShard(m);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} remove RPC failed on DataStore {}", shardNum, targetID);
            }
            shardMapLock.lock();
            primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
            replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
            replicaRatios = shardToReplicaRatioMap.get(shardNum);
            NotifyReplicaRemovedMessage nm = NotifyReplicaRemovedMessage.newBuilder().setDsID(targetID).setShard(shardNum).build();
            try {
                NotifyReplicaRemovedResponse r = dataStoreStubsMap.get(primaryDataStore).notifyReplicaRemoved(nm);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} removal notification RPC for DataStore {} failed on primary DataStore {}", shardNum, targetID, primaryDataStore);
            }
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            zkCurator.setZKShardDescription(shardNum, primaryDataStore, zkShardDescription.cloudName, zkShardDescription.versionNumber, replicaDataStores, replicaRatios);
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
                serverCpuUsageMap.put(response.getDsID(), response.getServerCPUUsage());
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
        return new Triplet<>(qpsMap, memoryUsagesMap, serverCpuUsageMap);
    }

    /** Take in maps from shards to loads, return a map from DSIDs to shards assigned to that datastore and their ratios. **/
    public Map<Integer, Map<Integer, Double>> getShardAssignments(Map<Integer, Integer> qpsLoad, Map<Integer, Integer> memoryLoad) {
        int numShards = shardToPrimaryDataStoreMap.size();
        List<Integer> aliveDSIDs = dataStoresMap.keySet().stream()
                .filter(i -> dataStoresMap.get(i).status.get() == DataStoreDescription.ALIVE)
                .collect(Collectors.toList());
        int numServers = aliveDSIDs.size();
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
        for(int dsID: aliveDSIDs) {
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
            statisticsLock.lock();
            logger.info("Query Statistics: {}", queryStatistics);
            serverShardRatios = lb.balanceLoad(numShards, numServers, shardLoads, shardMemoryUsages, currentLocations, queryStatistics, maxMemory);
            statisticsLock.lock();
        } catch (IloException e) {
            logger.info("Cplex exception");
            e.printStackTrace();
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
    public void assignShards(Map<Integer, Map<Integer, Double>> assignmentMap, Map<Integer, Integer> loadMap) {
        List<Thread> dsThreads = new ArrayList<>();
        // Do additions for all datastores in parallel.
        for(Map.Entry<Integer, Map<Integer, Double>> entry: assignmentMap.entrySet()) {
            Thread t = new Thread(() -> {
                // For each datastore, do additions sequentially.
                int dsID = entry.getKey();
                List<Integer> addReplicasList = new ArrayList<>();
                Map<Integer, Double> ratioMap = entry.getValue();
                // Get a list of shards to add to this datastore.
                for (Map.Entry<Integer, Double> assignment : ratioMap.entrySet()) {
                    int shardNum = assignment.getKey();
                    double shardRatio = assignment.getValue();
                    if (shardRatio > 0.0) {
                        if (shardToPrimaryDataStoreMap.get(shardNum) != dsID) {
                            addReplicasList.add(shardNum);
                        }
                    }
                }
                // Sort in decreasing order by load on the new replica.
                addReplicasList = addReplicasList.stream().sorted(Comparator.comparing(i -> -1 * ratioMap.get(i) * loadMap.get(i))).collect(Collectors.toList());
                // Sequentially do additions from most-loaded to least-loaded.
                for (Integer shardNum: addReplicasList) {
                    addReplica(shardNum, dsID, ratioMap.get(shardNum));
                }
            });
            t.start();
            dsThreads.add(t);
        }
        dsThreads.forEach(t -> { try {t.join(); } catch (InterruptedException ignored) {} });
        // Do all removals sequentially.
        for(Map.Entry<Integer, Map<Integer, Double>> entry: assignmentMap.entrySet()) {
            int dsID = entry.getKey();
            for (Map.Entry<Integer, Double> assignment : entry.getValue().entrySet()) {
                int shardNum = assignment.getKey();
                double shardRatio = assignment.getValue();
                if (shardRatio == 0.0) {
                    if (shardToPrimaryDataStoreMap.get(shardNum) == dsID) {
                        removeShard(shardNum, dsID);
                    }
                    if (shardToReplicaDataStoreMap.get(shardNum).contains(dsID)) {
                        removeShard(shardNum, dsID);
                    }
                }
            }
        }
    }

    private Integer findShardForDataStore(int dsID) {
        for (Map.Entry<Integer, Integer> primaryEntry: shardToPrimaryDataStoreMap.entrySet()) {
            if (primaryEntry.getValue() == dsID) {
                return primaryEntry.getKey();
            }
        }
        for (Map.Entry<Integer, List<Integer>> replicaEntry: shardToReplicaDataStoreMap.entrySet()) {
            if (replicaEntry.getValue().contains(dsID)) {
                return replicaEntry.getKey();
            }
        }
        return null;
    }

    public void killDataStore(int dsID) {
        DataStoreDescription dsDescription = dataStoresMap.get(dsID);
        if (dsDescription.status.compareAndSet(DataStoreDescription.ALIVE, DataStoreDescription.DEAD)) {
            logger.warn("DS{} Failure Detected", dsID);
            zkCurator.setDSDescription(dsDescription);
            Integer shardToRemove;
            do {
                shardMapLock.lock();
                shardToRemove = findShardForDataStore(dsID);
                shardMapLock.unlock();
                if (shardToRemove != null) {
                    removeShard(shardToRemove, dsID);
                }
            } while (shardToRemove != null);
        }
    }

    public void autoScale(Map<Integer, Double> serverCpuUsage) {
        OptionalDouble averageCpuUsageOpt = serverCpuUsage.values().stream().mapToDouble(i -> i).average();
        if (averageCpuUsageOpt.isEmpty()) {
            return;
        }
        double averageCpuUsage = averageCpuUsageOpt.getAsDouble();
        logger.info("Average CPU Usage: {}", averageCpuUsage);
        // Add a server.
        if (averageCpuUsage > 0.8) {
            logger.info("Adding DataStore");
            boolean success = cCloud.addDataStore();
            if (!success) {
                logger.info("DataStore addition failed");
            }
        }
        // Remove a server.
        if (averageCpuUsage < 0.5) {
            List<Integer> removeableDSIDs = dataStoresMap.keySet().stream()
                    .filter(i -> dataStoresMap.get(i).status.get() == DataStoreDescription.ALIVE)
                    .filter(i -> dsIDToCloudID.containsKey(i))
                    .collect(Collectors.toList());
            if (removeableDSIDs.size() > 0) {
                Map<Integer, Integer> primaryCount = new HashMap<>();
                removeableDSIDs.forEach(i -> primaryCount.put(i, 0));
                shardMapLock.lock();
                for (Integer primaryDSID: shardToPrimaryDataStoreMap.values()) {
                    if (removeableDSIDs.contains(primaryDSID)) {
                        primaryCount.merge(primaryDSID, 1, Integer::sum);
                    }
                }
                shardMapLock.unlock();
                int removedDSID = primaryCount.keySet().stream().min(Comparator.comparing(primaryCount::get)).get();
                int removedCloudID = dsIDToCloudID.get(removedDSID);
                logger.info("Remove DataStore: {} Cloud ID: {}", removedDSID, removedCloudID);
                killDataStore(removedDSID);
                cCloud.removeDataStore(removedCloudID);
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
                Triplet<Map<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Double>> load = collectLoad();
                Map<Integer, Integer> qpsLoad = load.getValue0();
                Map<Integer, Integer> memoryUsages = load.getValue1();
                logger.info("Collected QPS Load: {}", qpsLoad);
                logger.info("Collected memory usages: {}", memoryUsages);
                Map<Integer, Map<Integer, Double>> assignmentMap = getShardAssignments(qpsLoad, memoryUsages);
                logger.info("Generated assignment map: {}", assignmentMap);
                assignShards(assignmentMap, qpsLoad);
                Map<Integer, Double> serverCpuUsage = load.getValue2();
                logger.info("Collected DataStore CPU Usage: {}", serverCpuUsage);
                if (cCloud != null) {
                    autoScale(serverCpuUsage);
                }
            }
        }
    }
}

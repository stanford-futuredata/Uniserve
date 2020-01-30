package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

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

    private boolean runLoadBalancerDaemon = true;
    private final LoadBalancerDaemon loadBalancer;
    private final static int loadBalancerSleepDurationMillis = 100;

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

    public boolean addReplica(int shardNum, int replicaID) {
        int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        assert (replicaID != primaryDataStore);
        assert (!replicaDataStores.contains(replicaID));
        CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(replicaID);
        LoadShardReplicaMessage m = LoadShardReplicaMessage.newBuilder().setShard(shardNum).build();
        try {
            LoadShardReplicaResponse r = stub.loadShardReplica(m);
            if (r.getReturnCode() != 0) {
                logger.warn("Shard {} load failed on DataStore {}", shardNum, replicaID);
                return false;
            }
        } catch (StatusRuntimeException e) {
            logger.warn("Shard {} load RPC failed on DataStore {}", shardNum, replicaID);
            return false;
        }
        replicaDataStores.add(replicaID);
        zkCurator.setShardReplicas(shardNum, replicaDataStores);
        return true;
    }

    public void removeShard(int shardNum, int targetID) {
        int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
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
            replicaDataStores.remove(newPrimary);
            shardToPrimaryDataStoreMap.put(shardNum, newPrimary);
            zkCurator.setShardReplicas(shardNum, replicaDataStores);
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            zkCurator.setZKShardDescription(shardNum, newPrimary, zkShardDescription.cloudName, zkShardDescription.versionNumber);
        } else {
            assert(replicaDataStores.contains(targetID));
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(targetID);
            RemoveShardMessage m = RemoveShardMessage.newBuilder().setShard(shardNum).build();
            try {
                RemoveShardResponse r = stub.removeShard(m);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} remove RPC failed on DataStore {}", shardNum, targetID);
                assert(false);
            }
            replicaDataStores.remove(Integer.valueOf(targetID));
            zkCurator.setShardReplicas(shardNum, replicaDataStores);
        }
    }

    private class LoadBalancerDaemon extends Thread {
        @Override
        public void run() {
            while (runLoadBalancerDaemon) {
                // TODO:  Do something.
                try {
                    Thread.sleep(loadBalancerSleepDurationMillis);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}

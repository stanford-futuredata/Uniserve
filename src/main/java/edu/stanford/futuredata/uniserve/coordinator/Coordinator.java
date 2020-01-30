package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    private boolean runReplicationDaemon = true;
    private final ReplicationDaemon replicationDaemon;
    private final static int replicationDaemonSleepDurationMillis = 100;

    public Coordinator(String zkHost, int zkPort, String coordinatorHost, int coordinatorPort) {
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        zkCurator = new CoordinatorCurator(zkHost, zkPort);
        this.server = ServerBuilder.forPort(coordinatorPort)
                .addService(new ServiceDataStoreCoordinator(this))
                .addService(new ServiceBrokerCoordinator(this))
                .build();
        replicationDaemon = new ReplicationDaemon();
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
        replicationDaemon.start();
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
        runReplicationDaemon = false;
        try {
            replicationDaemon.interrupt();
            replicationDaemon.join();
        } catch (InterruptedException ignored) {}
    }

    private boolean addReplica(int shardNum, int replicaID) {
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
        shardToReplicaDataStoreMap.put(shardNum, Collections.singletonList(replicaID));
        return true;
    }

    private class ReplicationDaemon extends Thread {
        @Override
        public void run() {
            // TODO:  Make sane and robust.
            while (runReplicationDaemon) {
                List<Thread> replicationThreads = new ArrayList<>();
                for (Map.Entry<Integer, Integer> shardVersionEntry : shardToVersionMap.entrySet()) {
                    Thread replicationThread = new Thread(() -> {
                        int shardNum = shardVersionEntry.getKey();
                        int shardVersion = shardVersionEntry.getValue();
                        if (shardVersion > 0) {
                            int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
                            if (dataStoresMap.size() > 1 && shardToReplicaDataStoreMap.get(shardNum).size() == 0) {
                                int replicaID = (primaryDataStore + 1) % dataStoresMap.size();
                                addReplica(shardNum, replicaID);
                            }
                        }
                    });
                    replicationThread.start();
                    replicationThreads.add(replicationThread);
                }
                for (int i = 0; i < replicationThreads.size(); i++) {
                    try {
                        replicationThreads.get(i).join();
                    } catch (InterruptedException e) {
                        i--;
                    }
                }
                try {
                    Thread.sleep(replicationDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}

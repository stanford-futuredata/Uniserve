package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
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
    private final CoordinatorCurator zkCurator;

    // Used to assign each datastore a unique incremental ID.
    private final AtomicInteger dataStoreNumber = new AtomicInteger(0);
    // Map from datastore IDs to their descriptions.
    private final Map<Integer, DataStoreDescription> dataStoresMap = new ConcurrentHashMap<>();
    // Map from shards to last uploaded versions.
    private final Map<Integer, Integer> shardToVersionMap = new ConcurrentHashMap<>();
    // Map from shards to their primaries.
    private final Map<Integer, Integer> shardToPrimaryDataStoreMap = new ConcurrentHashMap<>();
    // Map from shards to their replicas.
    private final Map<Integer, List<Integer>> shardToReplicaDataStoreMap = new ConcurrentHashMap<>();

    private boolean runReplicationDaemon = true;
    private final ReplicationDaemon replicationDaemon;
    private final static int replicationDaemonSleepDurationMillis = 1000;

    public Coordinator(String zkHost, int zkPort, String coordinatorHost, int coordinatorPort) {
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        zkCurator = new CoordinatorCurator(zkHost, zkPort);
        this.server = ServerBuilder.forPort(coordinatorPort).addService(new DataStoreCoordinatorService())
                .addService(new BrokerCoordinatorService())
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
        runReplicationDaemon = false;
        try {
            replicationDaemon.interrupt();
            replicationDaemon.join();
        } catch (InterruptedException ignored) {}
    }

    private class ReplicationDaemon extends Thread {
        @Override
        public void run() {
            // TODO:  Make sane and robust.
            while (runReplicationDaemon) {
                List<Thread> replicationThreadList = new ArrayList<>();
                for (Map.Entry<Integer, Integer> shardVersionEntry : shardToVersionMap.entrySet()) {
                    Thread replicationThread = new Thread(() -> {
                        int shardNum = shardVersionEntry.getKey();
                        int shardVersion = shardVersionEntry.getValue();
                        if (shardVersion > 0) {
                            int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
                            List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
                            if (dataStoresMap.size() > 1 && replicaDataStores.size() == 0) {
                                int replicaID = (primaryDataStore + 1) % dataStoresMap.size();
                                assert (replicaID != primaryDataStore);
                                DataStoreDescription dsDesc = dataStoresMap.get(replicaID);
                                CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dsDesc.stub;
                                LoadShardReplicaMessage m = LoadShardReplicaMessage.newBuilder().setShard(shardNum).build();
                                LoadShardReplicaResponse r = stub.loadShardReplica(m);
                                if (r.getReturnCode() != 0) {
                                    logger.warn("Shard {} load failed on DataStore {}", shardNum, replicaID);
                                    return;
                                }
                                shardToReplicaDataStoreMap.put(shardNum, Collections.singletonList(replicaID));
                                zkCurator.setShardReplicas(shardNum, Collections.singletonList(replicaID));
                            }
                        }
                    });
                    replicationThread.start();
                    replicationThreadList.add(replicationThread);
                }
                for (int i = 0; i < replicationThreadList.size(); i++) {
                    try {
                        replicationThreadList.get(i).join();
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

    private class DataStoreCoordinatorService extends DataStoreCoordinatorGrpc.DataStoreCoordinatorImplBase {

        @Override
        public void registerDataStore(RegisterDataStoreMessage request,
                                      StreamObserver<RegisterDataStoreResponse> responseObserver) {
            responseObserver.onNext(registerDataStoreHandler(request));
            responseObserver.onCompleted();
        }

        private RegisterDataStoreResponse registerDataStoreHandler(RegisterDataStoreMessage m) {
            String host = m.getHost();
            int port = m.getPort();
            int dsID = dataStoreNumber.getAndIncrement();
            dataStoresMap.put(dsID, new DataStoreDescription(host, port));
            zkCurator.setDSDescription(dsID, host, port);
            logger.info("Registered DataStore ID: {} Host: {} Port: {}", dsID, host, port);
            return RegisterDataStoreResponse.newBuilder().setReturnCode(0).setDataStoreID(dsID).build();
        }

        @Override
        public void shardUpdate(ShardUpdateMessage request, StreamObserver<ShardUpdateResponse> responseObserver) {
            responseObserver.onNext(shardUpdateHandler(request));
            responseObserver.onCompleted();
        }

        private ShardUpdateResponse shardUpdateHandler(ShardUpdateMessage m) {
            int shardNum = m.getShardNum();
            String cloudName = m.getShardCloudName();
            int versionNumber = m.getVersionNumber();
            int dsID = shardToPrimaryDataStoreMap.get(shardNum);
            shardToVersionMap.put(shardNum, versionNumber);
            zkCurator.setZKShardDescription(shardNum, dsID, cloudName, versionNumber);
            logger.info("Uploaded Shard {} Version {}", shardNum, versionNumber);
            return ShardUpdateResponse.newBuilder().setReturnCode(0).build();
        }
    }

    private class BrokerCoordinatorService extends BrokerCoordinatorGrpc.BrokerCoordinatorImplBase {

        @Override
        public void shardLocation(ShardLocationMessage request,
                                      StreamObserver<ShardLocationResponse> responseObserver) {
            responseObserver.onNext(shardLocationHandler(request));
            responseObserver.onCompleted();
        }

        private int assignShardToDataStore(int shardNum) {
            // TODO:  Better DataStore choice.
            return shardNum % dataStoresMap.size();
        }

        private ShardLocationResponse shardLocationHandler(ShardLocationMessage m) {
            int shardNum = m.getShard();
            // Check if the shard's location is known.
            Integer dsID = shardToPrimaryDataStoreMap.getOrDefault(shardNum, null);
            if (dsID != null) {
                DataStoreDescription dsDesc = dataStoresMap.get(dsID);
                String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
                return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
            }
            // If not, assign it to a DataStore.
            int newDSID = assignShardToDataStore(shardNum);
            dsID = shardToPrimaryDataStoreMap.putIfAbsent(shardNum, newDSID);
            if (dsID != null) {
                DataStoreDescription dsDesc = dataStoresMap.get(dsID);
                String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
                return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
            }
            shardToReplicaDataStoreMap.put(shardNum, new ArrayList<>());
            shardToVersionMap.put(shardNum, 0);
            // Tell the DataStore to create the shard.
            DataStoreDescription dsDesc = dataStoresMap.get(newDSID);
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dsDesc.stub;
            CreateNewShardMessage cns = CreateNewShardMessage.newBuilder().setShard(shardNum).build();
            CreateNewShardResponse cnsResponse = stub.createNewShard(cns);
            assert cnsResponse.getReturnCode() == 0; //TODO:  Error handling.
            // Once the shard is created, add it to the ZooKeeper map.
            zkCurator.setZKShardDescription(m.getShard(), newDSID, Utilities.null_name, 0);
            zkCurator.setShardReplicas(m.getShard(), new ArrayList<>());
            String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
            return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
        }

    }

}

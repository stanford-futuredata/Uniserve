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
import java.util.stream.Collectors;

public class Coordinator {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final String coordinatorHost;
    private final int coordinatorPort;
    private final Server server;
    private final CoordinatorCurator zkCurator;

    // List of all known datastores.  Servers can be added, but never removed.
    private final List<DataStoreDescription> dataStoresList = new ArrayList<>();
    // Map from shards to datastores, defined as indices into dataStoresList.
    private final Map<Integer, Integer> shardToVersionMap = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> shardToPrimaryDataStoreMap = new ConcurrentHashMap<>();
    private final Map<Integer, List<Integer>> shardToReplicaDataStoreMap = new ConcurrentHashMap<>();
    private boolean runReplicationDaemon = true;
    private final ReplicationDaemon replicationDaemon;
    private final static int replicationDaemonSleepDurationMillis = 1000;

    public Coordinator(String zkHost, int zkPort, String coordinatorHost, int coordinatorPort) {
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        zkCurator = new CoordinatorCurator(zkHost, zkPort);
        ServerBuilder serverBuilder = ServerBuilder.forPort(coordinatorPort);
        this.server = serverBuilder.addService(new DataStoreCoordinatorService())
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
            while (runReplicationDaemon) {
                List<Thread> replicationThreadList = new ArrayList<>();
                for (Map.Entry<Integer, Integer> shardVersionEntry : shardToVersionMap.entrySet()) {
                    Thread replicationThread = new Thread(() -> {
                        int shardNum = shardVersionEntry.getKey();
                        int shardVersion = shardVersionEntry.getValue();
                        if (shardVersion > 0) {
                            int primaryDataStore = shardToPrimaryDataStoreMap.get(shardNum);
                            List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
                            if (dataStoresList.size() > 1 && replicaDataStores.size() == 0) {
                                int newReplicaDataStore = (primaryDataStore + 1) % dataStoresList.size();
                                assert (newReplicaDataStore != primaryDataStore);
                                DataStoreDescription dsDesc = dataStoresList.get(newReplicaDataStore);
                                CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dsDesc.stub;
                                LoadExistingShardMessage m = LoadExistingShardMessage.newBuilder().setShard(shardNum).build();
                                LoadExistingShardResponse r = stub.loadExistingShard(m);
                                if (r.getReturnCode() != 0) {
                                    logger.warn("Shard {} load failed on DataStore {}", shardNum, newReplicaDataStore);
                                    return;
                                }
                                shardToReplicaDataStoreMap.put(shardNum, Collections.singletonList(newReplicaDataStore));
                                try {
                                    zkCurator.setShardReplicas(shardNum, Collections.singletonList(dsDesc.connectString));
                                } catch (Exception e) {
                                    logger.error("Shard {} ZK Replica Set Failed", shardNum);
                                }
                            }
                        }
                    });
                    replicationThread.start();
                    replicationThreadList.add(replicationThread);
                }
                for (Thread replicationThread: replicationThreadList) {
                    try {
                        replicationThread.join();
                    } catch (InterruptedException e) {
                        return;
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
            dataStoresList.add(new DataStoreDescription(host, port));
            logger.info("Registered DataStore {} {}", host, port);
            return RegisterDataStoreResponse.newBuilder().setReturnCode(0).build();
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
            String connectString = dataStoresList.get(shardToPrimaryDataStoreMap.get(shardNum)).connectString;
            shardToVersionMap.put(shardNum, versionNumber);
            try {
                zkCurator.setZKShardDescription(shardNum, connectString, cloudName, versionNumber);
            } catch (Exception e) {
                logger.error("Error updating connection string in ZK: {}", e.getMessage());
                return ShardUpdateResponse.newBuilder().setReturnCode(1).build();
            }
            logger.info("Uploaded Shard {} Version {}", cloudName, versionNumber);
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
            return shardNum % dataStoresList.size();
        }

        private ShardLocationResponse shardLocationHandler(ShardLocationMessage m) {
            int shardNum = m.getShard();
            // Check if the shard's location is known.
            Integer dsNum = shardToPrimaryDataStoreMap.getOrDefault(shardNum, null);
            if (dsNum != null) {
                DataStoreDescription dsDesc = dataStoresList.get(dsNum);
                String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
                return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
            }
            // If not, assign it to a DataStore.
            int chosenDataStore = assignShardToDataStore(shardNum);
            dsNum = shardToPrimaryDataStoreMap.putIfAbsent(shardNum, chosenDataStore);
            if (dsNum != null) {
                DataStoreDescription dsDesc = dataStoresList.get(dsNum);
                String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
                return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
            }
            shardToReplicaDataStoreMap.put(shardNum, new ArrayList<>());
            shardToVersionMap.put(shardNum, 0);
            // Tell the DataStore to create the shard.
            DataStoreDescription dsDesc = dataStoresList.get(chosenDataStore);
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dsDesc.stub;
            CreateNewShardMessage cns = CreateNewShardMessage.newBuilder().setShard(shardNum).build();
            CreateNewShardResponse cnsResponse = stub.createNewShard(cns);
            assert cnsResponse.getReturnCode() == 0; //TODO:  Error handling.
            String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
            // Once the shard is created, add it to the ZooKeeper map.
            try {
                zkCurator.setZKShardDescription(m.getShard(), connectString, Utilities.null_name, 0);
            } catch (Exception e) {
                logger.error("Error adding connection string to ZK: {}", e.getMessage());
            }
            try {
                zkCurator.setShardReplicas(m.getShard(), new ArrayList<>());
            } catch (Exception e) {
                logger.error("Error adding replicas to ZK: {}", e.getMessage());
            }
            return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
        }

    }

}

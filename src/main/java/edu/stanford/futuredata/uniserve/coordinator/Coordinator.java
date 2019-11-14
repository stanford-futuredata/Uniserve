package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final String coordinatorHost;
    private final int coordinatorPort;
    private final Server server;
    private final CoordinatorCurator zkCurator;

    // List of all known datastores.  Servers can be added, but never removed.
    private final List<DataStoreDescription> dataStoresList = new ArrayList<>();
    // Map from shards to datastores, defined as indices into dataStoresList.
    private final Map<Integer, Integer> shardToDataStoreMap = new ConcurrentHashMap<>();

    public Coordinator(String zkHost, int zkPort, String coordinatorHost, int coordinatorPort) {
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        zkCurator = new CoordinatorCurator(zkHost, zkPort);
        ServerBuilder serverBuilder = ServerBuilder.forPort(coordinatorPort);
        this.server = serverBuilder.addService(new DataStoreCoordinatorService())
                .addService(new BrokerCoordinatorService())
                .build();
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
        return 0;
    }

    /** Stop serving requests and shutdown resources. */
    public void stopServing() {
        if (server != null) {
            server.shutdown();
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
            logger.info("Registered {} {}", host, port);
            return RegisterDataStoreResponse.newBuilder().setReturnCode(0).build();
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
            Integer dsNum = shardToDataStoreMap.getOrDefault(shardNum, null);
            if (dsNum != null) {
                DataStoreDescription dsDesc = dataStoresList.get(dsNum);
                String connectString = String.format("%s:%d", dsDesc.getHost(), dsDesc.getPort());
                return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
            }
            // If not, assign it to a DataStore.
            int chosenDataStore = assignShardToDataStore(shardNum);
            dsNum = shardToDataStoreMap.putIfAbsent(shardNum, chosenDataStore);
            if (dsNum != null) {
                DataStoreDescription dsDesc = dataStoresList.get(dsNum);
                String connectString = String.format("%s:%d", dsDesc.getHost(), dsDesc.getPort());
                return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
            }
            // Tell the DataStore to create the shard.
            DataStoreDescription dsDesc = dataStoresList.get(chosenDataStore);
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dsDesc.getStub();
            CreateNewShardMessage cns = CreateNewShardMessage.newBuilder().setShard(shardNum).build();
            CreateNewShardResponse cnsResponse = stub.createNewShard(cns);
            assert cnsResponse.getReturnCode() == 0; //TODO:  Error handling.
            String connectString = String.format("%s:%d", dsDesc.getHost(), dsDesc.getPort());
            // Once the shard is created, add it to the ZooKeeper map.
            try {
                zkCurator.setShardConnectString(m.getShard(), connectString);
            } catch (Exception e) {
                logger.error("Error adding connection string to ZK: {}", e.getMessage());
            }
            return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
        }

    }

}

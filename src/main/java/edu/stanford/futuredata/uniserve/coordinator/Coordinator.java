package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Coordinator {

    private final int port;
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    private final Server server;
    private final CoordinatorCurator zkCurator;

    // List of all known datastores.  Servers can be added, but never removed.
    private final List<DataStoreDescription> dataStoresList = new ArrayList<>();
    // Map from shards to datastores, defined as indices into dataStoresList.
    private final Map<Integer, Integer> shardToDataStoreMap = new HashMap<>();

    public Coordinator(String zkHost, int zkPort, int coordinatorPort) {
        this.port = coordinatorPort;
        zkCurator = new CoordinatorCurator(zkHost, zkPort);
        ServerBuilder serverBuilder = ServerBuilder.forPort(port);
        this.server = serverBuilder.addService(new DataStoreCoordinatorService())
                .addService(new BrokerCoordinatorService())
                .build();
    }

    /** Start serving requests. */
    public int startServing() {
        try {
            server.start();
            zkCurator.registerCoordinator("localhost", port);
        } catch (Exception e) {
            this.stopServing();
            return 1;
        }
        logger.info("Coordinator server started, listening on " + port);
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

        private ShardLocationResponse shardLocationHandler(ShardLocationMessage m) {
            // TODO:  Add synchronization.
            int shardNum = m.getShard();
            if (shardToDataStoreMap.containsKey(shardNum)) {
                DataStoreDescription dsDesc = dataStoresList.get(shardToDataStoreMap.get(shardNum));
                String connectString = String.format("%s:%d", dsDesc.getHost(), dsDesc.getPort());
                return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
            } else {
                int chosenDataStore = shardNum % dataStoresList.size(); // TODO:  Better assignment.
                shardToDataStoreMap.put(shardNum, chosenDataStore);
                DataStoreDescription dsDesc = dataStoresList.get(chosenDataStore);
                CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dsDesc.getStub();
                CreateNewShardMessage cns = CreateNewShardMessage.newBuilder().setShard(shardNum).build();
                CreateNewShardResponse cnsResponse = stub.createNewShard(cns);
                assert cnsResponse.getReturnCode() == 0; //TODO:  Error handling.
                String connectString = String.format("%s:%d", dsDesc.getHost(), dsDesc.getPort());
                try {
                    zkCurator.setShardConnectString(m.getShard(), connectString);
                } catch (Exception e) {
                    logger.error("Error adding connection string to ZK: {}", e.getMessage());
                }
                return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
            }
        }

    }

}

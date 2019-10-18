package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Coordinator {

    private final int port;
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    private final Server server;
    private final CoordinatorCurator zkCurator;

    private List<Pair<String, Integer>> dataStoresList = new ArrayList<>();

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
            dataStoresList.add(new Pair<>(host, port));
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
            String host = dataStoresList.get(0).getValue0();
            int port = dataStoresList.get(0).getValue1();
            String connectString = String.format("%s:%d", host, port);
            try {
                zkCurator.setShardConnectString(m.getShard(), connectString);
            } catch (Exception e) {
                logger.error("Error adding connection string to ZK: {}", e.getMessage());
            }
            return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
        }

    }

}

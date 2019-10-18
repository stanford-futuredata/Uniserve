package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.DataStoreCoordinatorGrpc;
import edu.stanford.futuredata.uniserve.RegisterDataStoreMessage;
import edu.stanford.futuredata.uniserve.RegisterDataStoreResponse;
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

    public Coordinator(int port) {
        this.port = port;
        zkCurator = new CoordinatorCurator("localhost", 2181);
        ServerBuilder serverBuilder = ServerBuilder.forPort(port);
        this.server = serverBuilder.addService(new DataStoreCoordinatorService())
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

}

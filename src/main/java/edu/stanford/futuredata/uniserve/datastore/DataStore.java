package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.AddRowAck;
import edu.stanford.futuredata.uniserve.QueryDataGrpc;
import edu.stanford.futuredata.uniserve.Row;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStore {

    private final int port;
    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);
    private final Server server;

    public DataStore(int port, ShardFactory shardFactory) {
        this.port = port;
        ServerBuilder serverBuilder = ServerBuilder.forPort(port);
        this.server = serverBuilder.addService(new QueryDataService(shardFactory, logger))
                .build();
    }

    /** Start serving requests. */
    public void startServing() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                DataStore.this.stopServing();
                System.err.println("*** server shut down");
            }
        });
    }

    /** Stop serving requests and shutdown resources. */
    public void stopServing() {
        if (server != null) {
            server.shutdown();
        }
    }


    private static class QueryDataService extends QueryDataGrpc.QueryDataImplBase {

        private final ShardFactory shardFactory;
        private final Logger logger;

        QueryDataService(ShardFactory shardFactory, Logger logger) {
            this.shardFactory = shardFactory;
            this.logger = logger;
        }

        @Override
        public void addRow(Row request, StreamObserver<AddRowAck> responseObserver) {
            responseObserver.onNext(addRowHandler(request));
            responseObserver.onCompleted();
        }

        private AddRowAck addRowHandler(Row row) {
            logger.info("Row {}", row.getShard());
            return AddRowAck.newBuilder().setReturnCode(0).build();
        }

    }
}

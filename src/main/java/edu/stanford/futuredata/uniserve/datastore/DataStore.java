package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStore {

    private final int port;
    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);
    private final Server server;
    private final Shard shard;

    public DataStore(int port, ShardFactory shardFactory) {
        this.port = port;
        this.shard = shardFactory.createShard();
        ServerBuilder serverBuilder = ServerBuilder.forPort(port);
        this.server = serverBuilder.addService(new QueryDataService())
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


    private class QueryDataService extends QueryDataGrpc.QueryDataImplBase {

        @Override
        public void insertRow(InsertRowMessage request, StreamObserver<InsertRowResponse> responseObserver) {
            responseObserver.onNext(addRowHandler(request));
            responseObserver.onCompleted();
        }

        private InsertRowResponse addRowHandler(InsertRowMessage row) {
            ByteString rowData = row.getRowData();
            shard.addRow(rowData);
            return InsertRowResponse.newBuilder().setReturnCode(0).build();
        }

        @Override
        public void readQuery(ReadQueryMessage request,  StreamObserver<ReadQueryResponse> responseObserver) {
            responseObserver.onNext(readQueryHandler(request));
            responseObserver.onCompleted();
        }

        private ReadQueryResponse readQueryHandler(ReadQueryMessage readQuery) {
            ByteString serializedQuery = readQuery.getSerializedQuery();
            ByteArrayInputStream bis = new ByteArrayInputStream(serializedQuery.toByteArray());
            QueryPlan queryPlan;
            try {
                ObjectInput in = new ObjectInputStream(bis);
                queryPlan = (QueryPlan) in.readObject();
                in.close();
            } catch (IOException | ClassNotFoundException e) {
                logger.error("Query Deserialization Failed: {}", e.getMessage());
                return ReadQueryResponse.newBuilder().setReturnCode(1).build();
            }
            ByteString queryResponse = queryPlan.queryShard(shard);
            return ReadQueryResponse.newBuilder().setReturnCode(0).setResponse(queryResponse).build();

        }

    }
}

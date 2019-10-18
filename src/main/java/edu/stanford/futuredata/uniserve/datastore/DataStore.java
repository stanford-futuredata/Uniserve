package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Optional;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStore {

    private final int port;
    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);
    private final Server server;
    private final Shard shard;
    private final DataStoreCurator zkCurator;

    private String coordinatorHost;
    private int coordinatorPort;

    public DataStore(int port, ShardFactory shardFactory) {
        this.port = port;
        this.shard = shardFactory.createShard();
        ServerBuilder serverBuilder = ServerBuilder.forPort(port);
        this.server = serverBuilder.addService(new BrokerDataStoreService())
                .build();
        this.zkCurator = new DataStoreCurator("localhost", 2181);
    }

    /** Start serving requests. */
    public int startServing() {
        Optional<Pair<String, Integer>> hostPort = zkCurator.getMasterLocation();
        if (hostPort.isPresent()) {
            this.coordinatorHost = hostPort.get().getValue0();
            this.coordinatorPort = hostPort.get().getValue1();
        } else {
            return 1;
        }
        try {
            server.start();
        } catch (IOException e) {
            return 1;
        }
        logger.info("DataStore server started, listening on " + port);
        ManagedChannelBuilder channelBuilder =
                ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        DataStoreCoordinatorGrpc.DataStoreCoordinatorBlockingStub blockingStub =
                DataStoreCoordinatorGrpc.newBlockingStub(channel);
        RegisterDataStoreMessage m = RegisterDataStoreMessage.newBuilder().setHost("localhost").setPort(port).build();
        try {
            RegisterDataStoreResponse r = blockingStub.registerDataStore(m);
            assert r.getReturnCode() == 0;
        } catch (StatusRuntimeException e) {
            logger.error("Coordinator Unreachable: {}", e.getStatus());
            stopServing();
            return 1;
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                DataStore.this.stopServing();
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


    private class BrokerDataStoreService extends BrokerDataStoreGrpc.BrokerDataStoreImplBase {

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

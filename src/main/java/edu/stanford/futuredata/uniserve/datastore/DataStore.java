package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DataStore<R extends Row, S extends Shard<R>> {

    private final int port;
    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);
    private final Map<Integer, S> shardMap = new ConcurrentHashMap<>();
    private final Server server;
    private final DataStoreCurator zkCurator;
    private final ShardFactory<S> shardFactory;


    public DataStore(int port, ShardFactory<S> shardFactory) {
        this.port = port;
        this.shardFactory = shardFactory;
        ServerBuilder serverBuilder = ServerBuilder.forPort(port);
        this.server = serverBuilder.addService(new BrokerDataStoreService())
                .addService(new CoordinatorDataStoreService())
                .build();
        this.zkCurator = new DataStoreCurator("localhost", 2181);
    }

    /** Start serving requests. */
    public int startServing() {
        // Start serving.
        try {
            server.start();
        } catch (IOException e) {
            logger.warn("DataStore startup failed: {}", e.getMessage());
            return 1;
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                DataStore.this.shutDown();
            }
        });
        // Notify the coordinator of startup.
        Optional<Pair<String, Integer>> hostPort = zkCurator.getMasterLocation();
        int coordinatorPort;
        String coordinatorHost;
        if (hostPort.isPresent()) {
            coordinatorHost = hostPort.get().getValue0();
            coordinatorPort = hostPort.get().getValue1();
        } else {
            logger.warn("DataStore--Coordinator lookup failed");
            shutDown();
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
            channel.shutdown();
            shutDown();
            return 1;
        }
        channel.shutdown();
        return 0;
    }

    /** Stop serving requests and shutdown resources. */
    public void shutDown() {
        if (server != null) {
            server.shutdown();
        }
        for (Map.Entry<Integer, S> entry: shardMap.entrySet()) {
            entry.getValue().destroy();
            shardMap.remove(entry.getKey());
        }
    }

    private class BrokerDataStoreService extends BrokerDataStoreGrpc.BrokerDataStoreImplBase {

        @Override
        public void insertRow(InsertRowMessage request, StreamObserver<InsertRowResponse> responseObserver) {
            responseObserver.onNext(addRowHandler(request));
            responseObserver.onCompleted();
        }

        private InsertRowResponse addRowHandler(InsertRowMessage rowMessage) {
            int shardNum = rowMessage.getShard();
            if (shardMap.containsKey(shardNum)) {
                ByteString rowData = rowMessage.getRowData();
                R row;
                try {
                    row = (R) Utilities.byteStringToObject(rowData);
                } catch (IOException | ClassNotFoundException e) {
                    logger.error("Row Deserialization Failed: {}", e.getMessage());
                    return InsertRowResponse.newBuilder().setReturnCode(1).build();
                }
                int addRowReturnCode = shardMap.get(shardNum).addRow(row);
                return InsertRowResponse.newBuilder().setReturnCode(addRowReturnCode).build();
            } else {
                logger.warn("Got read request for absent shard {}", shardNum);
                return InsertRowResponse.newBuilder().setReturnCode(1).build();
            }
        }

        @Override
        public void readQuery(ReadQueryMessage request,  StreamObserver<ReadQueryResponse> responseObserver) {
            responseObserver.onNext(readQueryHandler(request));
            responseObserver.onCompleted();
        }

        private ReadQueryResponse readQueryHandler(ReadQueryMessage readQuery) {
            int shardNum = readQuery.getShard();
            if (shardMap.containsKey(shardNum)) {
                ByteString serializedQuery = readQuery.getSerializedQuery();
                QueryPlan<S, Serializable, Object> queryPlan;
                try {
                    queryPlan = (QueryPlan<S, Serializable, Object>) Utilities.byteStringToObject(serializedQuery);
                } catch (IOException | ClassNotFoundException e) {
                    logger.error("Query Deserialization Failed: {}", e.getMessage());
                    return ReadQueryResponse.newBuilder().setReturnCode(1).build();
                }
                Serializable queryResult = queryPlan.queryShard(shardMap.get(shardNum));
                ByteString queryResponse;
                try {
                    queryResponse = Utilities.objectToByteString(queryResult);
                } catch (IOException e) {
                    logger.error("Result Serialization Failed: {}", e.getMessage());
                    return ReadQueryResponse.newBuilder().setReturnCode(1).build();
                }
                return ReadQueryResponse.newBuilder().setReturnCode(0).setResponse(queryResponse).build();
            } else {
                logger.warn("Got read request for absent shard {}", shardNum);
                return ReadQueryResponse.newBuilder().setReturnCode(1).build();
            }
        }
    }

    private class CoordinatorDataStoreService extends CoordinatorDataStoreGrpc.CoordinatorDataStoreImplBase {

        @Override
        public void createNewShard(CreateNewShardMessage request, StreamObserver<CreateNewShardResponse> responseObserver) {
            responseObserver.onNext(createNewShardHandler(request));
            responseObserver.onCompleted();
        }

        private CreateNewShardResponse createNewShardHandler(CreateNewShardMessage request) {
            int shardNum = request.getShard();
            Optional<S> shard = shardFactory.createShard();
            if (shard.isEmpty()) {
                logger.error("Shard creation failed {}", shardNum);
                return CreateNewShardResponse.newBuilder().setReturnCode(1).build();
            }
            shardMap.put(shardNum, shard.get());
            logger.info("Created new shard {}", shardNum);
            return CreateNewShardResponse.newBuilder().setReturnCode(0).build();
        }
    }
}

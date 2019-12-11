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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DataStore<R extends Row, S extends Shard<R>> {

    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);

    private final String dsHost;
    private final int dsPort;
    private final Map<Integer, S> primaryShardMap = new ConcurrentHashMap<>();
    private final Map<Integer, S> replicaShardMap = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> shardVersionMap = new ConcurrentHashMap<>();
    private final Server server;
    private final DataStoreCurator zkCurator;
    private final ShardFactory<S> shardFactory;
    private final DataStoreCloud dsCloud;
    private final Path baseDirectory;
    private DataStoreCoordinatorGrpc.DataStoreCoordinatorBlockingStub coordinatorStub = null;
    private ManagedChannel coordinatorChannel = null;
    private boolean runUploadShardDaemon = true;
    private final UploadShardDaemon uploadShardDaemon;
    private final static int uploadThreadSleepDurationMillis = 1000;


    public DataStore(DataStoreCloud dsCloud, ShardFactory<S> shardFactory, Path baseDirectory, String zkHost, int zkPort, String dsHost, int dsPort) {
        this.dsHost = dsHost;
        this.dsPort = dsPort;
        this.dsCloud = dsCloud;
        this.shardFactory = shardFactory;
        this.baseDirectory = baseDirectory;
        ServerBuilder serverBuilder = ServerBuilder.forPort(dsPort);
        this.server = serverBuilder.addService(new BrokerDataStoreService())
                .addService(new CoordinatorDataStoreService())
                .build();
        this.zkCurator = new DataStoreCurator(zkHost, zkPort);
        uploadShardDaemon = new UploadShardDaemon();
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
        logger.info("DataStore server started, listening on " + dsPort);
        ManagedChannelBuilder channelBuilder =
                ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort).usePlaintext();
        coordinatorChannel = channelBuilder.build();
        coordinatorStub = DataStoreCoordinatorGrpc.newBlockingStub(coordinatorChannel);
        RegisterDataStoreMessage m = RegisterDataStoreMessage.newBuilder().setHost(dsHost).setPort(dsPort).build();
        try {
            RegisterDataStoreResponse r = coordinatorStub.registerDataStore(m);
            assert r.getReturnCode() == 0;
        } catch (StatusRuntimeException e) {
            logger.error("Coordinator Unreachable: {}", e.getStatus());
            shutDown();
            return 1;
        }
        if (dsCloud != null) {
            uploadShardDaemon.start();
        }
        return 0;
    }

    /** Stop serving requests and shutdown resources. */
    public void shutDown() {
        coordinatorChannel.shutdown();
        server.shutdown();
        if (dsCloud != null) {
            runUploadShardDaemon = false;
            try {
                uploadShardDaemon.interrupt();
                uploadShardDaemon.join();
            } catch (InterruptedException ignored) {
            }
        }
        for (Map.Entry<Integer, S> entry: primaryShardMap.entrySet()) {
            entry.getValue().destroy();
            primaryShardMap.remove(entry.getKey());
        }
    }

    /** Synchronously upload a shard to the cloud, returning its name and version number. **/
    public Optional<Pair<String, Integer>> uploadShardToCloud(int shardNum) {
        Shard<R> shard = primaryShardMap.get(shardNum);
        Optional<Path> shardDirectory = shard.shardToData();
        if (shardDirectory.isEmpty()) {
            logger.warn("Shard {} serialization failed", shardNum);
            return Optional.empty();
        }
        Optional<String> cloudName = dsCloud.uploadShardToCloud(shardDirectory.get(), Integer.toString(shardNum));
        if (cloudName.isEmpty()) {
            logger.warn("Shard {} upload failed", shardNum);
            return Optional.empty();
        }
        Integer versionNumber = shardVersionMap.get(shardNum).incrementAndGet();
        return Optional.of(new Pair<>(cloudName.get(), versionNumber));
    }

    /** Synchronously download a shard from the cloud **/
    public Optional<S> downloadShardFromCloud(int shardNum, String cloudName, int versionNumber) {
        Path downloadDirectory = Path.of(baseDirectory.toString(), Integer.toString(versionNumber));
        File downloadDirFile = downloadDirectory.toFile();
        if (!downloadDirFile.exists()) {
            boolean mkdirs = downloadDirFile.mkdirs();
            if (!mkdirs) {
                logger.warn("Shard {} version {} mkdirs failed", shardNum, versionNumber);
                return Optional.empty();
            }
        }
        int downloadReturnCode = dsCloud.downloadShardFromCloud(downloadDirectory, cloudName);
        if (downloadReturnCode != 0) {
            logger.warn("Shard {} download failed", shardNum);
            return Optional.empty();
        }
        Path targetDirectory = Path.of(downloadDirectory.toString(), cloudName);
        return shardFactory.createShardFromDir(targetDirectory);
    }

    private class UploadShardDaemon extends Thread {
        @Override
        public void run() {
            while (runUploadShardDaemon) {
                List<Thread> uploadThreadList = new ArrayList<>();
                for (Integer shardNum : primaryShardMap.keySet()) {
                    Thread uploadThread = new Thread(() -> {
                        Optional<Pair<String, Integer>> nameVersion = uploadShardToCloud(shardNum);
                        if (nameVersion.isEmpty()) {
                            return; // TODO:  Error handling.
                        }
                        ShardUpdateMessage shardUpdateMessage = ShardUpdateMessage.newBuilder().setShardNum(shardNum).setShardCloudName(nameVersion.get().getValue0())
                                .setVersionNumber(nameVersion.get().getValue1()).build();
                        try {
                            ShardUpdateResponse shardUpdateResponse = coordinatorStub.shardUpdate(shardUpdateMessage);
                            assert (shardUpdateResponse.getReturnCode() == 0); // TODO:  Error handling.
                        } catch (StatusRuntimeException e) {
                            logger.warn("ShardUpdateResponse RPC Failure {}", e.getMessage());
                        }
                    });
                    uploadThread.start();
                    uploadThreadList.add(uploadThread);
                }
                for (Thread uploadThread: uploadThreadList) {
                    try {
                        uploadThread.join();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                try {
                    Thread.sleep(uploadThreadSleepDurationMillis);
                } catch (InterruptedException e) {
                    return;
                }
            }
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
            if (primaryShardMap.containsKey(shardNum)) {
                ByteString rowData = rowMessage.getRowData();
                R row;
                try {
                    row = (R) Utilities.byteStringToObject(rowData);
                } catch (IOException | ClassNotFoundException e) {
                    logger.error("Row Deserialization Failed: {}", e.getMessage());
                    return InsertRowResponse.newBuilder().setReturnCode(1).build();
                }
                int addRowReturnCode = primaryShardMap.get(shardNum).addRow(row);
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
            S queriedShard = replicaShardMap.getOrDefault(shardNum, null);
            if (queriedShard == null) {
                queriedShard = primaryShardMap.getOrDefault(shardNum, null);
            }
            if (queriedShard != null) {
                ByteString serializedQuery = readQuery.getSerializedQuery();
                QueryPlan<S, Serializable, Object> queryPlan;
                try {
                    queryPlan = (QueryPlan<S, Serializable, Object>) Utilities.byteStringToObject(serializedQuery);
                } catch (IOException | ClassNotFoundException e) {
                    logger.error("Query Deserialization Failed: {}", e.getMessage());
                    return ReadQueryResponse.newBuilder().setReturnCode(1).build();
                }
                Serializable queryResult = queryPlan.queryShard(queriedShard);
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
            assert (!primaryShardMap.containsKey(shardNum));
            assert (!replicaShardMap.containsKey(shardNum));
            int shardVersionNumber = 0;
            Path shardPath = Path.of(baseDirectory.toString(), Integer.toString(shardVersionNumber), Integer.toString(shardNum));
            File shardPathFile = shardPath.toFile();
            if (!shardPathFile.exists()) {
                boolean mkdirs = shardPathFile.mkdirs();
                if (!mkdirs) {
                    logger.error("Shard directory creation failed {}", shardNum);
                    return CreateNewShardResponse.newBuilder().setReturnCode(1).build();
                }
            }
            Optional<S> shard = shardFactory.createNewShard(shardPath);
            if (shard.isEmpty()) {
                logger.error("Shard creation failed {}", shardNum);
                return CreateNewShardResponse.newBuilder().setReturnCode(1).build();
            }
            shardVersionMap.put(shardNum, new AtomicInteger(shardVersionNumber));
            primaryShardMap.put(shardNum, shard.get());
            logger.info("Created new primary shard {}", shardNum);
            return CreateNewShardResponse.newBuilder().setReturnCode(0).build();
        }

        @Override
        public void loadExistingShard(LoadExistingShardMessage request, StreamObserver<LoadExistingShardResponse> responseObserver) {
            responseObserver.onNext(loadExistingShardHandler(request));
            responseObserver.onCompleted();
        }

        private LoadExistingShardResponse loadExistingShardHandler(LoadExistingShardMessage request) {
            int shardNum = request.getShard();
            assert(!primaryShardMap.containsKey(shardNum));
            assert(!replicaShardMap.containsKey(shardNum));
            Optional<Pair<String, Integer>> cloudNameVersion = zkCurator.getShardCloudNameVersion(shardNum);
            if (cloudNameVersion.isEmpty()) {
                logger.error("Loading shard not in ZK: {}", shardNum);
                return LoadExistingShardResponse.newBuilder().setReturnCode(1).build();
            }
            String cloudName = cloudNameVersion.get().getValue0();
            int versionNumber = cloudNameVersion.get().getValue1();
            Optional<S> loadedShard = downloadShardFromCloud(shardNum, cloudName, versionNumber);
            if (loadedShard.isEmpty()) {
                logger.error("Shard load failed {}", shardNum);
                return LoadExistingShardResponse.newBuilder().setReturnCode(1).build();
            }
            replicaShardMap.put(shardNum, loadedShard.get());
            logger.info("Loaded new replica shard {}", shardNum);
            return LoadExistingShardResponse.newBuilder().setReturnCode(0).build();
        }
    }
}

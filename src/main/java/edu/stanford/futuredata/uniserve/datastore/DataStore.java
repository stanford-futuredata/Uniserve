package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.*;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataStore<R extends Row, S extends Shard> {

    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);

    // Datastore metadata
    private int dsID;
    private final String dsHost;
    private final int dsPort;

    // Map from shard number to shard data structure for primary shards.
    private final Map<Integer, S> primaryShardMap = new ConcurrentHashMap<>();
    // Map from shard number to shard data structure for replica shards.
    private final Map<Integer, S> replicaShardMap = new ConcurrentHashMap<>();
    // Map from shard number to shard version number.
    private final Map<Integer, Integer> shardVersionMap = new ConcurrentHashMap<>();
    // Map from shard number to last uploaded version number.
    private final Map<Integer, Integer> lastUploadedVersionMap = new ConcurrentHashMap<>();
    // Map from shard number to access lock.
    private final Map<Integer, ReadWriteLock> shardLockMap = new ConcurrentHashMap<>();

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
        this.server = ServerBuilder.forPort(dsPort).addService(new BrokerDataStoreService())
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
        coordinatorChannel =
                ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort).usePlaintext().build();
        coordinatorStub = DataStoreCoordinatorGrpc.newBlockingStub(coordinatorChannel);
        RegisterDataStoreMessage m = RegisterDataStoreMessage.newBuilder().setHost(dsHost).setPort(dsPort).build();
        try {
            RegisterDataStoreResponse r = coordinatorStub.registerDataStore(m);
            assert r.getReturnCode() == 0;
            this.dsID = r.getDataStoreID();
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
        server.shutdown();
        if (dsCloud != null) {
            runUploadShardDaemon = false;
            try {
                uploadShardDaemon.interrupt();
                uploadShardDaemon.join();
            } catch (InterruptedException ignored) {
            }
        }
        coordinatorChannel.shutdown();
        for (Map.Entry<Integer, S> entry: primaryShardMap.entrySet()) {
            entry.getValue().destroy();
            primaryShardMap.remove(entry.getKey());
        }
    }

    /** Synchronously upload a shard to the cloud, returning its name and version number. **/
    public Optional<Pair<String, Integer>> uploadShardToCloud(int shardNum) {
        Shard shard = primaryShardMap.get(shardNum);
        shardLockMap.get(shardNum).readLock().lock();
        Integer versionNumber = shardVersionMap.get(shardNum);
        Optional<Path> shardDirectory = shard.shardToData();
        if (shardDirectory.isEmpty()) {
            logger.warn("DS{} Shard {} serialization failed", dsID, shardNum);
            shardLockMap.get(shardNum).readLock().unlock();
            return Optional.empty();
        }
        Optional<String> cloudName = dsCloud.uploadShardToCloud(shardDirectory.get(), Integer.toString(shardNum));
        shardLockMap.get(shardNum).readLock().unlock();  // TODO:  Might block writes for too long.
        if (cloudName.isEmpty()) {
            logger.warn("DS{} Shard {} upload failed", dsID, shardNum);
            return Optional.empty();
        }
        return Optional.of(new Pair<>(cloudName.get(), versionNumber));
    }

    /** Synchronously download a shard from the cloud **/
    public Optional<S> downloadShardFromCloud(int shardNum, String cloudName, int versionNumber) {
        Path downloadDirectory = Path.of(baseDirectory.toString(), Integer.toString(versionNumber));
        File downloadDirFile = downloadDirectory.toFile();
        if (!downloadDirFile.exists()) {
            boolean mkdirs = downloadDirFile.mkdirs();
            if (!mkdirs) {
                logger.warn("DS{} Shard {} version {} mkdirs failed: {}", dsID, shardNum, versionNumber, downloadDirFile.getAbsolutePath());
                return Optional.empty();
            }
        }
        int downloadReturnCode = dsCloud.downloadShardFromCloud(downloadDirectory, cloudName);
        if (downloadReturnCode != 0) {
            logger.warn("DS{} Shard {} download failed", dsID, shardNum);
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
                    if (lastUploadedVersionMap.get(shardNum).equals(shardVersionMap.get(shardNum))) {
                        continue;
                    }
                    Thread uploadThread = new Thread(() -> {
                        Optional<Pair<String, Integer>> nameVersion = uploadShardToCloud(shardNum);
                        if (nameVersion.isEmpty()) {
                            return; // TODO:  Error handling.
                        }
                        ShardUpdateMessage shardUpdateMessage = ShardUpdateMessage.newBuilder().setShardNum(shardNum).setShardCloudName(nameVersion.get().getValue0())
                                .setVersionNumber(nameVersion.get().getValue1()).build();
                        try {
                            ShardUpdateResponse shardUpdateResponse = coordinatorStub.shardUpdate(shardUpdateMessage);
                            assert (shardUpdateResponse.getReturnCode() == 0);
                        } catch (StatusRuntimeException e) {
                            logger.warn("DS{} ShardUpdateResponse RPC Failure {}", dsID, e.getMessage());
                        }
                        lastUploadedVersionMap.put(shardNum, nameVersion.get().getValue1());
                    });
                    uploadThread.start();
                    uploadThreadList.add(uploadThread);
                }
                for (int i = 0; i < uploadThreadList.size(); i++) {
                    try {
                        uploadThreadList.get(i).join();
                    } catch (InterruptedException e) {
                        i--;
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

        private Map<Integer, CommitLockerThread> activeCLTs = new HashMap<>();

        private class CommitLockerThread extends Thread {
            // Holds a shard's write lock in between precommit and commit.
            public final Integer shardNum;
            public final WriteQueryPlan<R, S> writeQueryPlan;
            private final Lock lock;
            private final long txID;

            public CommitLockerThread(Integer shardNum, WriteQueryPlan<R, S> writeQueryPlan, Lock lock, long txID) {
                this.shardNum = shardNum;
                this.writeQueryPlan = writeQueryPlan;
                this.lock = lock;
                this.txID = txID;
            }

            public void run() {
                lock.lock();
                // Notify the precommit thread that the shard lock is held.
                synchronized (writeQueryPlan) {
                    assert (activeCLTs.get(shardNum) == null);
                    activeCLTs.put(shardNum, this);
                    writeQueryPlan.notify();
                }
                // Block until the commit thread is ready to release the shard lock.
                // TODO:  Automatically abort and unlock if this isn't triggered for X seconds after a precommit.
                try {
                    synchronized (writeQueryPlan) {
                        assert (activeCLTs.get(shardNum) != null);
                        while(activeCLTs.get(shardNum) != null) {
                            writeQueryPlan.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error("DS{} Interrupted while getting lock: {}", dsID, e.getMessage());
                    assert(false);
                }
                lock.unlock();
            }

            public void acquireLock() {
                this.start();
                try {
                    synchronized (writeQueryPlan) {
                        while(!(activeCLTs.get(shardNum) == this)) {
                            writeQueryPlan.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error("DS{} Interrupted while getting lock: {}", dsID, e.getMessage());
                    assert(false);
                }
            }

            public void releaseLock() {
                assert(this.isAlive());
                synchronized (this.writeQueryPlan) {
                    activeCLTs.get(shardNum).writeQueryPlan.notify();
                    activeCLTs.put(shardNum, null);
                }
            }
        }

        @Override
        public void writeQueryPreCommit(WriteQueryPreCommitMessage request, StreamObserver<WriteQueryPreCommitResponse> responseObserver) {
            responseObserver.onNext(writeQueryPreCommitHandler(request));
            responseObserver.onCompleted();
        }

        private WriteQueryPreCommitResponse writeQueryPreCommitHandler(WriteQueryPreCommitMessage rowMessage) {
            int shardNum = rowMessage.getShard();
            long txID = rowMessage.getTxID();
            if (primaryShardMap.containsKey(shardNum)) {
                WriteQueryPlan<R, S> writeQueryPlan;
                List<R> rows;
                try {
                    writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(rowMessage.getSerializedQuery());
                    rows = Arrays.asList((R[]) Utilities.byteStringToObject(rowMessage.getRowData()));
                } catch (IOException | ClassNotFoundException e) {
                    logger.error("DS{} Query Deserialization Failed: {}", dsID, e.getMessage());
                    assert(false);
                    return WriteQueryPreCommitResponse.newBuilder().setReturnCode(1).build();
                }
                S shard = primaryShardMap.get(shardNum);
                // Use the CommitLockerThread to acquire the shard's write lock.
                CommitLockerThread commitLockerThread = new CommitLockerThread(shardNum, writeQueryPlan, shardLockMap.get(shardNum).writeLock(), txID);
                commitLockerThread.acquireLock();
                boolean querySuccess = writeQueryPlan.preCommit(shard, rows);
                int addRowReturnCode;
                if (querySuccess) {
                    addRowReturnCode = 0;
                } else {
                    addRowReturnCode = 1;
                }
                return WriteQueryPreCommitResponse.newBuilder().setReturnCode(addRowReturnCode).build();
            } else {
                logger.warn("DS{} Got write request for absent shard {}", dsID, shardNum);
                return WriteQueryPreCommitResponse.newBuilder().setReturnCode(1).build();
            }
        }

        @Override
        public void writeQueryCommit(WriteQueryCommitMessage request, StreamObserver<WriteQueryCommitResponse> responseObserver) {
            responseObserver.onNext(writeQueryCommitHandler(request));
            responseObserver.onCompleted();
        }

        private WriteQueryCommitResponse writeQueryCommitHandler(WriteQueryCommitMessage rowMessage) {
            int shardNum = rowMessage.getShard();
            CommitLockerThread commitCLT = activeCLTs.get(shardNum);
            assert(commitCLT != null);  // The commit locker thread holds the shard's write lock.
            assert(commitCLT.txID == rowMessage.getTxID());
            WriteQueryPlan<R, S> writeQueryPlan = commitCLT.writeQueryPlan;
            if (primaryShardMap.containsKey(shardNum)) {
                boolean commitOrAbort = rowMessage.getCommitOrAbort(); // Commit on true, abort on false.
                S shard = primaryShardMap.get(shardNum);
                if (commitOrAbort) {
                    writeQueryPlan.commit(shard);
                    shardVersionMap.merge(shardNum, 1, Integer::sum);  // Increment version number
                } else {
                    writeQueryPlan.abort(shard);
                }
                // Have the commit locker thread release the shard's write lock.
                commitCLT.releaseLock();
            } else {
                logger.error("DS{} Got valid commit request on absent shard {} (!!!!!)", dsID, shardNum);
                assert(false);
            }
            return WriteQueryCommitResponse.newBuilder().build();
        }

        @Override
        public void readQuery(ReadQueryMessage request,  StreamObserver<ReadQueryResponse> responseObserver) {
            responseObserver.onNext(readQueryHandler(request));
            responseObserver.onCompleted();
        }

        private ReadQueryResponse readQueryHandler(ReadQueryMessage readQuery) {
            int shardNum = readQuery.getShard();
            S shard = replicaShardMap.getOrDefault(shardNum, null);
            if (shard == null) {
                shard = primaryShardMap.getOrDefault(shardNum, null);
            }
            if (shard != null) {
                ByteString serializedQuery = readQuery.getSerializedQuery();
                ReadQueryPlan<S, Serializable, Object> readQueryPlan;
                try {
                    readQueryPlan = (ReadQueryPlan<S, Serializable, Object>) Utilities.byteStringToObject(serializedQuery);
                } catch (IOException | ClassNotFoundException e) {
                    logger.error("DS{} Query Deserialization Failed: {}", dsID, e.getMessage());
                    return ReadQueryResponse.newBuilder().setReturnCode(1).build();
                }
                shardLockMap.get(shardNum).readLock().lock();
                Serializable queryResult = readQueryPlan.queryShard(shard);
                shardLockMap.get(shardNum).readLock().unlock();
                ByteString queryResponse;
                try {
                    queryResponse = Utilities.objectToByteString(queryResult);
                } catch (IOException e) {
                    logger.error("DS{} Result Serialization Failed: {}", dsID, e.getMessage());
                    return ReadQueryResponse.newBuilder().setReturnCode(1).build();
                }
                return ReadQueryResponse.newBuilder().setReturnCode(0).setResponse(queryResponse).build();
            } else {
                logger.warn("DS{} Got read request for absent shard {}", dsID, shardNum);
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
            Path shardPath = Path.of(baseDirectory.toString(), Integer.toString(0), Integer.toString(shardNum));
            File shardPathFile = shardPath.toFile();
            if (!shardPathFile.exists()) {
                boolean mkdirs = shardPathFile.mkdirs();
                if (!mkdirs) {
                    logger.error("DS{} Shard directory creation failed {}", dsID, shardNum);
                    return CreateNewShardResponse.newBuilder().setReturnCode(1).build();
                }
            }
            Optional<S> shard = shardFactory.createNewShard(shardPath);
            if (shard.isEmpty()) {
                logger.error("DS{} Shard creation failed {}", dsID, shardNum);
                return CreateNewShardResponse.newBuilder().setReturnCode(1).build();
            }
            shardLockMap.put(shardNum, new ReentrantReadWriteLock());
            shardVersionMap.put(shardNum, 0);
            lastUploadedVersionMap.put(shardNum, 0);
            primaryShardMap.put(shardNum, shard.get());
            logger.info("DS{} Created new primary shard {}", dsID, shardNum);
            return CreateNewShardResponse.newBuilder().setReturnCode(0).build();
        }

        @Override
        public void loadShardReplica(LoadShardReplicaMessage request, StreamObserver<LoadShardReplicaResponse> responseObserver) {
            responseObserver.onNext(loadShardReplicaHandler(request));
            responseObserver.onCompleted();
        }

        private LoadShardReplicaResponse loadShardReplicaHandler(LoadShardReplicaMessage request) {
            int shardNum = request.getShard();
            assert(!primaryShardMap.containsKey(shardNum));
            assert(!replicaShardMap.containsKey(shardNum));
            Optional<Pair<String, Integer>> cloudNameVersion = zkCurator.getShardCloudNameVersion(shardNum);
            if (cloudNameVersion.isEmpty()) {
                logger.error("DS{} Loading shard not in ZK: {}", dsID, shardNum);
                return LoadShardReplicaResponse.newBuilder().setReturnCode(1).build();
            }
            String cloudName = cloudNameVersion.get().getValue0();
            int versionNumber = cloudNameVersion.get().getValue1();
            Optional<S> loadedShard = downloadShardFromCloud(shardNum, cloudName, versionNumber);
            if (loadedShard.isEmpty()) {
                logger.error("DS{} Shard load failed {}", dsID, shardNum);
                return LoadShardReplicaResponse.newBuilder().setReturnCode(1).build();
            }
            shardLockMap.put(shardNum, new ReentrantReadWriteLock());
            shardVersionMap.put(shardNum, versionNumber);
            replicaShardMap.put(shardNum, loadedShard.get());
            logger.info("DS{} Loaded new replica shard {}", dsID, shardNum);
            return LoadShardReplicaResponse.newBuilder().setReturnCode(0).build();
        }
    }
}

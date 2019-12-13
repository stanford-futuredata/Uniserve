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
import org.slf4j.helpers.Util;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataStore<R extends Row, S extends Shard> {

    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);

    // Datastore metadata
    private int dsID;
    private final String dsHost;
    private final int dsPort;

    // Map from primary shard number to shard data structure.
    private final Map<Integer, S> primaryShardMap = new ConcurrentHashMap<>();
    // Map from replica shard number to shard data structure.
    private final Map<Integer, S> replicaShardMap = new ConcurrentHashMap<>();
    // Map from shard number to shard version number.
    private final Map<Integer, Integer> shardVersionMap = new ConcurrentHashMap<>();
    // Map from primary shard number to last uploaded version number.
    private final Map<Integer, Integer> lastUploadedVersionMap = new ConcurrentHashMap<>();
    // Map from shard number to access lock.
    private final Map<Integer, ReadWriteLock> shardLockMap = new ConcurrentHashMap<>();
    // Map from shard number to maps from version number to write query and data.
    private final Map<Integer, Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>>> writeLog = new ConcurrentHashMap<>();
    // Map from primary shard number to list of replica stubs for that shard.
    private final Map<Integer, List<DataStoreDataStoreGrpc.DataStoreDataStoreStub>> replicaStubsMap = new ConcurrentHashMap<>();
    // Map from primary shard number to list of replica channels for that shard.
    private final Map<Integer, List<ManagedChannel>> replicaChannelsMap = new ConcurrentHashMap<>();

    private final Server server;
    private final DataStoreCurator zkCurator;
    private final ShardFactory<S> shardFactory;
    private final DataStoreCloud dsCloud;
    private final Path baseDirectory;
    private DataStoreCoordinatorGrpc.DataStoreCoordinatorBlockingStub coordinatorStub = null;
    private ManagedChannel coordinatorChannel = null;

    private boolean runUploadShardDaemon = true;
    private final UploadShardDaemon uploadShardDaemon;
    private final static int uploadThreadSleepDurationMillis = 100;


    public DataStore(DataStoreCloud dsCloud, ShardFactory<S> shardFactory, Path baseDirectory, String zkHost, int zkPort, String dsHost, int dsPort) {
        this.dsHost = dsHost;
        this.dsPort = dsPort;
        this.dsCloud = dsCloud;
        this.shardFactory = shardFactory;
        this.baseDirectory = baseDirectory;
        this.server = ServerBuilder.forPort(dsPort)
                .addService(new BrokerDataStoreService())
                .addService(new CoordinatorDataStoreService())
                .addService(new DataStoreDataStoreService())
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
        for (List<ManagedChannel> channels: replicaChannelsMap.values()) {
            for (ManagedChannel channel: channels) {
                channel.shutdown();
            }
        }
        for (Map.Entry<Integer, S> entry: primaryShardMap.entrySet()) {
            entry.getValue().destroy();
            primaryShardMap.remove(entry.getKey());
        }
    }

    /** Synchronously upload a shard to the cloud, returning its name and version number. **/
    public Optional<Pair<String, Integer>> uploadShardToCloud(int shardNum) {
        // TODO:  Safely delete old versions.
        Shard shard = primaryShardMap.get(shardNum);
        shardLockMap.get(shardNum).readLock().lock();
        Integer versionNumber = shardVersionMap.get(shardNum);
        Optional<Path> shardDirectory = shard.shardToData();
        if (shardDirectory.isEmpty()) {
            logger.warn("DS{} Shard {} serialization failed", dsID, shardNum);
            shardLockMap.get(shardNum).readLock().unlock();
            return Optional.empty();
        }
        Optional<String> cloudName = dsCloud.uploadShardToCloud(shardDirectory.get(), Integer.toString(shardNum), versionNumber);
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

    private class CommitLockerThread extends Thread {
        // Holds a shard's write lock in between precommit and commit.
        public final Integer shardNum;
        public final WriteQueryPlan<R, S> writeQueryPlan;
        public final List<R> rows;
        private final Lock lock;
        private final Map<Integer, CommitLockerThread> activeCLTs;
        private final long txID;

        public CommitLockerThread(Map<Integer, CommitLockerThread> activeCLTs, Integer shardNum, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows, Lock lock, long txID) {
            this.activeCLTs = activeCLTs;
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.rows = rows;
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

    private class BrokerDataStoreService extends BrokerDataStoreGrpc.BrokerDataStoreImplBase {

        private Map<Integer, CommitLockerThread> activeCLTs = new HashMap<>();

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
                writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(rowMessage.getSerializedQuery());
                rows = Arrays.asList((R[]) Utilities.byteStringToObject(rowMessage.getRowData()));
                S shard = primaryShardMap.get(shardNum);
                // Use the CommitLockerThread to acquire the shard's write lock.
                CommitLockerThread commitLockerThread = new CommitLockerThread(activeCLTs, shardNum, writeQueryPlan, rows, shardLockMap.get(shardNum).writeLock(), txID);
                commitLockerThread.acquireLock();
                List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs = replicaStubsMap.get(shardNum);
                int numReplicas = replicaStubs.size();
                ReplicaPreCommitMessage rm = ReplicaPreCommitMessage.newBuilder()
                        .setShard(shardNum)
                        .setSerializedQuery(rowMessage.getSerializedQuery())
                        .setRowData(rowMessage.getRowData())
                        .setTxID(txID)
                        .setVersionNumber(shardVersionMap.get(shardNum))
                        .build();
                AtomicInteger numReplicaSuccesses = new AtomicInteger(0);
                Semaphore semaphore = new Semaphore(0);
                for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub: replicaStubs) {
                    StreamObserver<ReplicaPreCommitResponse> responseObserver = new StreamObserver<>() {
                        @Override
                        public void onNext(ReplicaPreCommitResponse replicaPreCommitResponse) {
                            numReplicaSuccesses.incrementAndGet();
                            semaphore.release();
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.error("DS{} Replica PreCommit Error: {}", dsID, throwable.getMessage());
                            semaphore.release();
                        }

                        @Override
                        public void onCompleted() {
                        }
                    };
                    stub.replicaPreCommit(rm, responseObserver);
                }
                boolean primaryWriteSuccess = writeQueryPlan.preCommit(shard, rows);
                try {
                    semaphore.acquire(numReplicas);
                } catch (InterruptedException e) {
                    logger.error("DS{} Write Query Interrupted Shard {}: {}", dsID, shardNum, e.getMessage());
                    assert(false);
                }
                int addRowReturnCode;
                if (primaryWriteSuccess && numReplicaSuccesses.get() == numReplicas) {
                    addRowReturnCode = 0;
                } else {
                    addRowReturnCode = 1;
                }
                return WriteQueryPreCommitResponse.newBuilder().setReturnCode(addRowReturnCode).build();
            } else {
                logger.warn("DS{} Primary got write request for absent shard {}", dsID, shardNum);
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

                List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs = replicaStubsMap.get(shardNum);
                int numReplicas = replicaStubs.size();
                ReplicaCommitMessage rm = ReplicaCommitMessage.newBuilder()
                        .setShard(shardNum)
                        .setCommitOrAbort(commitOrAbort)
                        .setTxID(rowMessage.getTxID())
                        .build();
                Semaphore semaphore = new Semaphore(0);
                for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub: replicaStubs) {
                    StreamObserver<ReplicaCommitResponse> responseObserver = new StreamObserver<>() {
                        @Override
                        public void onNext(ReplicaCommitResponse replicaCommitResponse) {
                            semaphore.release();
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.error("DS{} Replica Commit Error: {}", dsID, throwable.getMessage());
                            semaphore.release();
                        }

                        @Override
                        public void onCompleted() {
                        }
                    };
                    stub.replicaCommit(rm, responseObserver);
                }
                if (commitOrAbort) {
                    writeQueryPlan.commit(shard);
                    int newVersionNumber = shardVersionMap.get(shardNum) + 1;
                    Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = writeLog.get(shardNum);
                    shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, commitCLT.rows));
                    shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
                } else {
                    writeQueryPlan.abort(shard);
                }
                try {
                    semaphore.acquire(numReplicas);
                } catch (InterruptedException e) {
                    logger.error("DS{} Write Query Interrupted Shard {}: {}", dsID, shardNum, e.getMessage());
                    assert(false);
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
                readQueryPlan = (ReadQueryPlan<S, Serializable, Object>) Utilities.byteStringToObject(serializedQuery);
                shardLockMap.get(shardNum).readLock().lock();
                Serializable queryResult = readQueryPlan.queryShard(shard);
                shardLockMap.get(shardNum).readLock().unlock();
                ByteString queryResponse;
                queryResponse = Utilities.objectToByteString(queryResult);
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
            writeLog.put(shardNum, new ConcurrentHashMap<>());
            replicaChannelsMap.put(shardNum, new ArrayList<>());
            replicaStubsMap.put(shardNum, new ArrayList<>());
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
            // Get shard info from ZK.
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            String cloudName = zkShardDescription.cloudName;
            int replicaVersion = zkShardDescription.versionNumber;
            int primaryDSID = zkShardDescription.primaryDSID;
            // Download the shard.
            Optional<S> loadedShard = downloadShardFromCloud(shardNum, cloudName, replicaVersion);
            if (loadedShard.isEmpty()) {
                logger.error("DS{} Shard load failed {}", dsID, shardNum);
                return LoadShardReplicaResponse.newBuilder().setReturnCode(1).build();
            }
            // Load but lock the replica until it has been bootstrapped.
            S shard = loadedShard.get();
            shardLockMap.put(shardNum, new ReentrantReadWriteLock());
            shardLockMap.get(shardNum).writeLock().lock();
            replicaShardMap.put(shardNum, loadedShard.get());

            // Set up a connection to the primary.
            Pair<String, Integer> primaryConnectString = zkCurator.getConnectStringFromDSID(primaryDSID);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(primaryConnectString.getValue0(), primaryConnectString.getValue1()).usePlaintext().build();
            DataStoreDataStoreGrpc.DataStoreDataStoreBlockingStub primaryBlockingStub = DataStoreDataStoreGrpc.newBlockingStub(channel);

            // Bootstrap the replica, bringing it up to the same version as the primary.
            while (true) {
                BootstrapReplicaMessage m = BootstrapReplicaMessage.newBuilder()
                        .setShard(shardNum)
                        .setVersionNumber(replicaVersion)
                        .setDsID(dsID)
                        .build();
                BootstrapReplicaResponse r;
                try {
                    r = primaryBlockingStub.bootstrapReplica(m);
                } catch (StatusRuntimeException e) {
                    channel.shutdown();
                    logger.error("DS{} Replica Shard {} could not sync primary {}: {}", dsID, shardNum, primaryDSID, e.getMessage());
                    return LoadShardReplicaResponse.newBuilder().setReturnCode(1).build();
                }
                assert (r.getReturnCode() == 0);
                int primaryVersion = r.getVersionNumber();
                if (replicaVersion == primaryVersion) {
                    // Loop until acknowledgement replica has caught up to primary.
                    break;
                } else {
                    // If not caught up, replay the primary's log.
                    List<WriteQueryPlan<R, S>> writeQueryPlans = Arrays.asList((WriteQueryPlan<R, S>[]) Utilities.byteStringToObject(r.getWriteQueries()));
                    List<R[]> rowsList = Arrays.asList((R[][]) Utilities.byteStringToObject(r.getWriteData()));
                    assert(writeQueryPlans.size() == rowsList.size());
                    for (int i = 0; i < writeQueryPlans.size(); i++) {
                        WriteQueryPlan<R, S> query = writeQueryPlans.get(i);
                        List<R> rows = Arrays.asList(rowsList.get(i));
                        assert(query.preCommit(shard, rows));
                        query.commit(shard);
                    }
                    replicaVersion = r.getVersionNumber();
                }
            }
            channel.shutdown();
            writeLog.put(shardNum, new ConcurrentHashMap<>());
            shardVersionMap.put(shardNum, replicaVersion);
            shardLockMap.get(shardNum).writeLock().unlock();
            logger.info("DS{} Loaded new replica shard {} version {}", dsID, shardNum, replicaVersion);
            return LoadShardReplicaResponse.newBuilder().setReturnCode(0).build();
        }
    }

    private class DataStoreDataStoreService extends DataStoreDataStoreGrpc.DataStoreDataStoreImplBase {

        @Override
        public void bootstrapReplica(BootstrapReplicaMessage request, StreamObserver<BootstrapReplicaResponse> responseObserver) {
            responseObserver.onNext(bootstrapReplicaHandler(request));
            responseObserver.onCompleted();
        }

        private BootstrapReplicaResponse bootstrapReplicaHandler(BootstrapReplicaMessage request) {
            int shardNum = request.getShard();
            shardLockMap.get(shardNum).readLock().lock();
            Integer replicaVersion = request.getVersionNumber();
            Integer primaryVersion = shardVersionMap.get(shardNum);
            assert(primaryVersion != null);
            assert(replicaVersion <= primaryVersion);
            Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = writeLog.get(shardNum);
            if (replicaVersion.equals(primaryVersion)) {
                Pair<String, Integer> connectString = zkCurator.getConnectStringFromDSID(request.getDsID());
                ManagedChannel channel = ManagedChannelBuilder.forAddress(connectString.getValue0(), connectString.getValue1()).usePlaintext().build();
                DataStoreDataStoreGrpc.DataStoreDataStoreStub asyncStub = DataStoreDataStoreGrpc.newStub(channel);
                replicaChannelsMap.get(shardNum).add(channel);
                replicaStubsMap.get(shardNum).add(asyncStub);
            }
            shardLockMap.get(shardNum).readLock().unlock();
            List<WriteQueryPlan<R, S>> writeQueryPlans = new ArrayList<>();
            List<R[]> rowListList = new ArrayList<>();
            for (int v = replicaVersion + 1; v <= primaryVersion; v++) {
                writeQueryPlans.add(shardWriteLog.get(v).getValue0());
                rowListList.add((R[]) shardWriteLog.get(v).getValue1().toArray(new Row[0]));
            }
            WriteQueryPlan<R, S>[] writeQueryPlansArray = writeQueryPlans.toArray(new WriteQueryPlan[0]);
            R[][] rowArrayArray = rowListList.toArray((R[][]) new Row[0][]);
            return BootstrapReplicaResponse.newBuilder().setReturnCode(0)
                    .setVersionNumber(primaryVersion)
                    .setWriteData(Utilities.objectToByteString(rowArrayArray))
                    .setWriteQueries(Utilities.objectToByteString(writeQueryPlansArray))
                    .build();
        }

        private Map<Integer, CommitLockerThread> activeCLTs = new HashMap<>();

        @Override
        public void replicaPreCommit(ReplicaPreCommitMessage request, StreamObserver<ReplicaPreCommitResponse> responseObserver) {
            responseObserver.onNext(replicaPreCommitHandler(request));
            responseObserver.onCompleted();
        }

        private ReplicaPreCommitResponse replicaPreCommitHandler(ReplicaPreCommitMessage request) {
            int shardNum = request.getShard();
            long txID = request.getTxID();
            if (replicaShardMap.containsKey(shardNum)) {
                WriteQueryPlan<R, S> writeQueryPlan;
                List<R> rows;
                writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(request.getSerializedQuery());
                rows = Arrays.asList((R[]) Utilities.byteStringToObject(request.getRowData()));
                S shard = replicaShardMap.get(shardNum);
                // Use the CommitLockerThread to acquire the shard's write lock.
                CommitLockerThread commitLockerThread = new CommitLockerThread(activeCLTs, shardNum, writeQueryPlan, rows, shardLockMap.get(shardNum).writeLock(), txID);
                commitLockerThread.acquireLock();
                assert(request.getVersionNumber() == shardVersionMap.get(shardNum));
                boolean replicaWriteSuccess = writeQueryPlan.preCommit(shard, rows);
                int returnCode;
                if (replicaWriteSuccess) {
                    returnCode = 0;
                } else {
                    returnCode = 1;
                }
                return ReplicaPreCommitResponse.newBuilder().setReturnCode(returnCode).build();
            } else {
                logger.warn("DS{} replica got write request for absent shard {}", dsID, shardNum);
                return ReplicaPreCommitResponse.newBuilder().setReturnCode(1).build();
            }
        }

        @Override
        public void replicaCommit(ReplicaCommitMessage request, StreamObserver<ReplicaCommitResponse> responseObserver) {
            responseObserver.onNext(replicaCommitHandler(request));
            responseObserver.onCompleted();
        }

        private ReplicaCommitResponse replicaCommitHandler(ReplicaCommitMessage request) {
            int shardNum = request.getShard();
            CommitLockerThread commitCLT = activeCLTs.get(shardNum);
            assert(commitCLT != null);  // The commit locker thread holds the shard's write lock.
            assert(commitCLT.txID == request.getTxID());
            WriteQueryPlan<R, S> writeQueryPlan = commitCLT.writeQueryPlan;
            if (replicaShardMap.containsKey(shardNum)) {
                boolean commitOrAbort = request.getCommitOrAbort(); // Commit on true, abort on false.
                S shard = replicaShardMap.get(shardNum);
                if (commitOrAbort) {
                    writeQueryPlan.commit(shard);
                    int newVersionNumber = shardVersionMap.get(shardNum) + 1;
                    Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = writeLog.get(shardNum);
                    shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, commitCLT.rows));
                    shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
                } else {
                    writeQueryPlan.abort(shard);
                }
                // Have the commit locker thread release the shard's write lock.
                commitCLT.releaseLock();
            } else {
                logger.error("DS{} Got valid commit request on absent shard {} (!!!!!)", dsID, shardNum);
                assert(false);
            }
            return ReplicaCommitResponse.newBuilder().build();
        }
    }
}

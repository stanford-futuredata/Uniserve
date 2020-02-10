package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import io.grpc.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;

public class DataStore<R extends Row, S extends Shard> {

    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);

    // Datastore metadata
    int dsID;
    private final String dsHost;
    private final int dsPort;

    // Map from primary shard number to shard data structure.
    public final Map<Integer, S> primaryShardMap = new ConcurrentHashMap<>(); // Public for testing.
    // Map from replica shard number to shard data structure.
    final Map<Integer, S> replicaShardMap = new ConcurrentHashMap<>();
    // Map from shard number to shard version number.
    final Map<Integer, Integer> shardVersionMap = new ConcurrentHashMap<>();
    // Map from primary shard number to last uploaded version number.
    final Map<Integer, Integer> lastUploadedVersionMap = new ConcurrentHashMap<>();
    // Map from shard number to access lock.
    final Map<Integer, ReadWriteLock> shardLockMap = new ConcurrentHashMap<>();
    // Map from shard number to maps from version number to write query and data.
    final Map<Integer, Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>>> writeLog = new ConcurrentHashMap<>();
    // Map from primary shard number to list of replica descriptions for that shard.
    final Map<Integer, List<ReplicaDescription>> replicaDescriptionsMap = new ConcurrentHashMap<>();
    // Map from Unix second timestamp to number of read queries made during that timestamp, per shard.
    final Map<Integer, Map<Long, Integer>> QPSMap = new ConcurrentHashMap<>();

    private final Server server;
    final DataStoreCurator zkCurator;
    final ShardFactory<S> shardFactory;
    private final DataStoreCloud dsCloud;
    final Path baseDirectory;
    private DataStoreCoordinatorGrpc.DataStoreCoordinatorBlockingStub coordinatorStub = null;
    private ManagedChannel coordinatorChannel = null;

    public boolean runUploadShardDaemon = true; // Public for testing.
    private final UploadShardDaemon uploadShardDaemon;
    public static int uploadThreadSleepDurationMillis = 10000;

    public static int qpsReportTimeInterval = 10;

    public boolean runPingDaemon = true; // Public for testing
    private final PingDaemon pingDaemon;
    private final static int pingDaemonSleepDurationMillis = 100;
    private final static int pingDaemonRefreshInterval = 10;


    public DataStore(DataStoreCloud dsCloud, ShardFactory<S> shardFactory, Path baseDirectory, String zkHost, int zkPort, String dsHost, int dsPort) {
        this.dsHost = dsHost;
        this.dsPort = dsPort;
        this.dsCloud = dsCloud;
        this.shardFactory = shardFactory;
        this.baseDirectory = baseDirectory;
        this.server = ServerBuilder.forPort(dsPort)
                .addService(new ServiceBrokerDataStore<>(this))
                .addService(new ServiceCoordinatorDataStore<>(this))
                .addService(new ServiceDataStoreDataStore<>(this))
                .build();
        this.zkCurator = new DataStoreCurator(zkHost, zkPort);
        uploadShardDaemon = new UploadShardDaemon();
        pingDaemon = new PingDaemon();
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
        pingDaemon.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                DataStore.this.shutDown();
            }
        });
        return 0;
    }

    /** Stop serving requests and shutdown resources. */
    public void shutDown() {
        server.shutdown();
        runPingDaemon = false;
        runUploadShardDaemon = false;
        try {
            pingDaemon.join();
        } catch (InterruptedException ignored) {}
        if (dsCloud != null) {
            try {
                uploadShardDaemon.interrupt();
                uploadShardDaemon.join();
            } catch (InterruptedException ignored) {}
        }
        for (List<ReplicaDescription> replicaDescriptions: replicaDescriptionsMap.values()) {
            for (ReplicaDescription rd: replicaDescriptions) {
                rd.channel.shutdown();
            }
        }
        coordinatorChannel.shutdown();
        for (Map.Entry<Integer, S> entry: primaryShardMap.entrySet()) {
            entry.getValue().destroy();
            primaryShardMap.remove(entry.getKey());
        }
    }

    /** Synchronously upload a shard to the cloud, returning its name and version number. **/
    /** Assumes read lock is held **/
    public void uploadShardToCloud(int shardNum) {
        // TODO:  Safely delete old versions.
        Shard shard = primaryShardMap.get(shardNum);
        Integer versionNumber = shardVersionMap.get(shardNum);
        // Load the shard's data into files.
        Optional<Path> shardDirectory = shard.shardToData();
        if (shardDirectory.isEmpty()) {
            logger.warn("DS{} Shard {} serialization failed", dsID, shardNum);
            return;
        }
        // Upload the shard's data.
        Optional<String> cloudName = dsCloud.uploadShardToCloud(shardDirectory.get(), Integer.toString(shardNum), versionNumber);
        if (cloudName.isEmpty()) {
            logger.warn("DS{} Shard {}-{} upload failed", dsID, shardNum, versionNumber);
            return;
        }
        // Notify the coordinator about the upload.
        ShardUpdateMessage shardUpdateMessage = ShardUpdateMessage.newBuilder().setShardNum(shardNum).setShardCloudName(cloudName.get())
                .setVersionNumber(versionNumber).build();
        try {
            ShardUpdateResponse shardUpdateResponse = coordinatorStub.shardUpdate(shardUpdateMessage);
            assert (shardUpdateResponse.getReturnCode() == 0);
        } catch (StatusRuntimeException e) {
            logger.warn("DS{} ShardUpdateResponse RPC Failure {}", dsID, e.getMessage());
        }
        lastUploadedVersionMap.put(shardNum, versionNumber);
        logger.warn("DS{} Shard {}-{} upload succeeded", dsID, shardNum, versionNumber);
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
                        shardLockMap.get(shardNum).readLock().lock();  // TODO:  Might block writes for too long.
                        if (primaryShardMap.containsKey(shardNum)) {
                            uploadShardToCloud(shardNum);
                        }
                        shardLockMap.get(shardNum).readLock().unlock();
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

    private class PingDaemon extends Thread {
        @Override
        public void run() {
            List<DataStoreDescription> dsDescriptions = new ArrayList<>();
            int runCount = 0;
            while (runPingDaemon) {
                if (runCount % pingDaemonRefreshInterval == 0) {
                    dsDescriptions = zkCurator.getOtherDSDescriptions(dsID);
                }
                if (dsDescriptions.size() > 0) {
                    DataStoreDescription dsToPing =
                            dsDescriptions.get(ThreadLocalRandom.current().nextInt(dsDescriptions.size()));
                    int pingedDSID = dsToPing.dsID;
                    assert(pingedDSID != dsID);
                    ManagedChannel dsChannel =
                            ManagedChannelBuilder.forAddress(dsToPing.host, dsToPing.port).usePlaintext().build();
                    DataStoreDataStoreGrpc.DataStoreDataStoreBlockingStub stub = DataStoreDataStoreGrpc.newBlockingStub(dsChannel);
                    DataStorePingMessage pm = DataStorePingMessage.newBuilder().build();
                    try {
                        DataStorePingResponse alwaysEmpty = stub.dataStorePing(pm);
                    } catch (StatusRuntimeException e) {
                        PotentialDSFailureMessage fm = PotentialDSFailureMessage.newBuilder().setDsID(pingedDSID).build();
                        PotentialDSFailureResponse alwaysEmpty = coordinatorStub.potentialDSFailure(fm);
                    }
                    dsChannel.shutdown();
                }
                runCount++;
                try {
                    Thread.sleep(pingDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}

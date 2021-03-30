package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import io.grpc.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class DataStore<R extends Row, S extends Shard> {

    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);

    // Datastore metadata
    public int dsID = -1;
    private int cloudID;
    private final String dsHost;
    private final int dsPort;
    final boolean readWriteAtomicity;
    public boolean serving = false;
    public boolean useReflink = false;

    // Map from shard number to shard data structure.
    public final Map<Integer, S> shardMap = new ConcurrentHashMap<>(); // Public for testing.
    // Map from shard number to old versions.  TODO:  Delete older versions.
    final Map<Integer, Map<Long, S>> multiVersionShardMap = new ConcurrentHashMap<>();
    // Map from shard number to shard version number.
    final Map<Integer, Integer> shardVersionMap = new ConcurrentHashMap<>();
    // Map from shard number to access lock.
    final Map<Integer, ShardLock> shardLockMap = new ConcurrentHashMap<>();
    // Map from shard number to maps from version number to write query and data.  TODO:  Clean older entries.
    final Map<Integer, Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>>> writeLog = new ConcurrentHashMap<>();
    // Map from primary shard number to list of replica descriptions for that shard.
    final Map<Integer, List<ReplicaDescription>> replicaDescriptionsMap = new ConcurrentHashMap<>();
    // Map from primary shard number to last timestamp known for the shard.
    final Map<Integer, Long> shardTimestampMap = new ConcurrentHashMap<>();
    // Map from Unix second timestamp to number of read queries made during that timestamp, per shard.
    final Map<Integer, Map<Long, Integer>> QPSMap = new ConcurrentHashMap<>();
    // Map from table names to tableInfos.
    private final Map<String, TableInfo> tableInfoMap = new ConcurrentHashMap<>();
    // Map from dsID to a ManagedChannel.
    private final Map<Integer, ManagedChannel> dsIDToChannelMap = new ConcurrentHashMap<>();

    private final Server server;
    final DataStoreCurator zkCurator;
    final ShardFactory<S> shardFactory;
    final DataStoreCloud dsCloud;
    final Path baseDirectory;
    private DataStoreCoordinatorGrpc.DataStoreCoordinatorBlockingStub coordinatorStub = null;
    private ManagedChannel coordinatorChannel = null;
    public final AtomicInteger ephemeralShardNum = new AtomicInteger(Integer.MAX_VALUE);

    public static int qpsReportTimeInterval = 60; // In seconds -- should be same as load balancer interval.

    public boolean runPingDaemon = true; // Public for testing
    private final PingDaemon pingDaemon;
    public final static int pingDaemonSleepDurationMillis = 100;
    private final static int pingDaemonRefreshInterval = 10;

    // Collect execution times of all read queries.
    public final Collection<Long> readQueryExecuteTimes = new ConcurrentLinkedQueue<>();
    public final Collection<Long> readQueryFullTimes = new ConcurrentLinkedQueue<>();

    public static final int COLLECT = 0;
    public static final int PREPARE = 1;
    public static final int COMMIT = 2;
    public static final int ABORT = 3;

    ConsistentHash consistentHash;

    public DataStore(DataStoreCloud dsCloud, ShardFactory<S> shardFactory, Path baseDirectory, String zkHost, int zkPort, String dsHost, int dsPort, int cloudID, boolean readWriteAtomicity) {
        this.dsHost = dsHost;
        this.dsPort = dsPort;
        this.dsCloud = dsCloud;
        this.readWriteAtomicity = readWriteAtomicity;
        this.shardFactory = shardFactory;
        this.baseDirectory = baseDirectory;
        this.server = ServerBuilder.forPort(dsPort)
                .addService(new ServiceBrokerDataStore<>(this))
                .addService(new ServiceCoordinatorDataStore<>(this))
                .addService(new ServiceDataStoreDataStore<>(this))
                .build();
        this.zkCurator = new DataStoreCurator(zkHost, zkPort);
        this.cloudID = cloudID;
        pingDaemon = new PingDaemon();
    }

    /** Start serving requests.
     * @return*/
    public boolean startServing() {
        // Start serving.
        assert(!serving);
        serving = true;
        try {
            server.start();
        } catch (IOException e) {
            logger.warn("DataStore startup failed: {}", e.getMessage());
            return false;
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
            return false;
        }
        logger.info("DataStore server started, listening on " + dsPort);
        coordinatorChannel =
                ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort).usePlaintext().build();
        coordinatorStub = DataStoreCoordinatorGrpc.newBlockingStub(coordinatorChannel);
        RegisterDataStoreMessage m = RegisterDataStoreMessage.newBuilder()
                .setHost(dsHost).setPort(dsPort).setCloudID(cloudID).build();
        try {
            RegisterDataStoreResponse r = coordinatorStub.registerDataStore(m);
            assert r.getReturnCode() == 0;
            this.dsID = r.getDataStoreID();
            ephemeralShardNum.set(Integer.MAX_VALUE - Broker.SHARDS_PER_TABLE * dsID);
        } catch (StatusRuntimeException e) {
            logger.error("Coordinator Unreachable: {}", e.getStatus());
            shutDown();
            return false;
        }
        pingDaemon.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                DataStore.this.shutDown();
            }
        });
        return true;
    }

    /** Stop serving requests and shutdown resources. */
    public void shutDown() {
        if (!serving) {
            return;
        }
        serving = false;
        server.shutdown();
        runPingDaemon = false;
        try {
            pingDaemon.join();
        } catch (InterruptedException ignored) {}
        for (List<ReplicaDescription> replicaDescriptions: replicaDescriptionsMap.values()) {
            for (ReplicaDescription rd: replicaDescriptions) {
                rd.channel.shutdownNow();
            }
        }
        coordinatorChannel.shutdownNow();
        for (Map.Entry<Integer, S> entry: shardMap.entrySet()) {
            entry.getValue().destroy();
            shardMap.remove(entry.getKey());
        }
        for (ManagedChannel c: dsIDToChannelMap.values()) {
            c.shutdownNow();
        }
        zkCurator.close();
        int numQueries = readQueryExecuteTimes.size();
        OptionalDouble averageExecuteTime = readQueryExecuteTimes.stream().mapToLong(i -> i).average();
        OptionalDouble averageFullTime = readQueryFullTimes.stream().mapToLong(i -> i).average();
        if (averageExecuteTime.isPresent() && averageFullTime.isPresent()) {
            long p50Exec = readQueryExecuteTimes.stream().mapToLong(i -> i).sorted().toArray()[readQueryExecuteTimes.size() / 2];
            long p99Exec = readQueryExecuteTimes.stream().mapToLong(i -> i).sorted().toArray()[readQueryExecuteTimes.size() * 99 / 100];
            long p50Full = readQueryFullTimes.stream().mapToLong(i -> i).sorted().toArray()[readQueryFullTimes.size() / 2];
            long p99Full = readQueryFullTimes.stream().mapToLong(i -> i).sorted().toArray()[readQueryFullTimes.size() * 99 / 100];
            logger.info("Queries: {} p50 Exec: {}μs p99 Exec: {}μs p50 Full: {}μs p99 Full: {}μs", numQueries, p50Exec, p99Exec, p50Full, p99Full);
        }
    }

    Optional<S> createNewShard(int shardNum) {
        Path shardPath = Path.of(baseDirectory.toString(), Integer.toString(0), Integer.toString(shardNum));
        File shardPathFile = shardPath.toFile();
        if (!shardPathFile.exists()) {
            boolean mkdirs = shardPathFile.mkdirs();
            if (!mkdirs) {
                logger.error("DS{} Shard directory creation failed {}", dsID, shardNum);
                return Optional.empty();
            }
        }
        return shardFactory.createNewShard(shardPath, shardNum);
    }

    /** Creates all metadata for a shard not yet seen on this server, creating the shard if it does not yet exist **/
    boolean createShardMetadata(int shardNum) {
        if (shardLockMap.containsKey(shardNum)) {
            return true;
        }
        ShardLock shardLock = new ShardLock();
        shardLock.systemLockLock();
        if (shardLockMap.putIfAbsent(shardNum, shardLock) == null) {
            assert (!shardMap.containsKey(shardNum));
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            if (zkShardDescription == null) {
                Optional<S> shard = createNewShard(shardNum);
                if (shard.isEmpty()) {
                    logger.error("DS{} Shard creation failed {}", dsID, shardNum);
                    shardLock.systemLockUnlock();
                    return false;
                }
                shardMap.put(shardNum, shard.get());
                shardVersionMap.put(shardNum, 0);
                logger.info("DS{} Created new primary shard {}", dsID, shardNum);
            } else {
                shardVersionMap.put(shardNum, zkShardDescription.versionNumber);
            }
            QPSMap.put(shardNum, new ConcurrentHashMap<>());
            writeLog.put(shardNum, new ConcurrentHashMap<>());
            replicaDescriptionsMap.put(shardNum, new ArrayList<>());
            multiVersionShardMap.put(shardNum, new ConcurrentHashMap<>());
        }
        shardLock.systemLockUnlock();
        return true;
    }

    /** Downloads the shard if not already present, evicting if necessary **/
    boolean ensureShardCached(int shardNum) {
        if (!shardMap.containsKey(shardNum)) {
            ZKShardDescription z = zkCurator.getZKShardDescription(shardNum);
            Optional<S> shard = downloadShardFromCloud(shardNum, z.cloudName, z.versionNumber);
            if (shard.isEmpty()) {
                return false;
            }
            shardMap.putIfAbsent(shardNum, shard.get());
            shardVersionMap.put(shardNum, z.versionNumber);
        }
        return true;
    }

    /** Synchronously upload a shard to the cloud; assumes shard write lock is held **/
    // TODO:  Safely delete old versions.
    public void uploadShardToCloud(int shardNum) {
        long uploadStart = System.currentTimeMillis();
        Integer versionNumber = shardVersionMap.get(shardNum);
        Shard shard = shardMap.get(shardNum);
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
        zkCurator.setZKShardDescription(shardNum, cloudName.get(), versionNumber);
        logger.info("DS{} Shard {}-{} upload succeeded. Time: {}ms", dsID, shardNum, versionNumber, System.currentTimeMillis() - uploadStart);
    }

    /** Synchronously download a shard from the cloud **/
    public Optional<S> downloadShardFromCloud(int shardNum, String cloudName, int versionNumber) {
        long downloadStart = System.currentTimeMillis();
        Path downloadDirectory = Path.of(baseDirectory.toString(), Integer.toString(versionNumber));
        File downloadDirFile = downloadDirectory.toFile();
        if (!downloadDirFile.exists()) {
            boolean mkdirs = downloadDirFile.mkdirs();
            if (!mkdirs && !downloadDirFile.exists()) {
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
        Optional<S> shard = shardFactory.createShardFromDir(targetDirectory, shardNum);
        logger.info("DS{} Shard {}-{} download succeeded. Time: {}ms", dsID, shardNum, versionNumber, System.currentTimeMillis() - downloadStart);
        return shard;
    }

    public Optional<S> copyShardToDir(int shardNum, String cloudName, int versionNumber) {
        long copyStart = System.currentTimeMillis();
        Shard shard = shardMap.get(shardNum);
        // Load the shard's data into files.
        Optional<Path> shardDirectory = shard.shardToData();
        if (shardDirectory.isEmpty()) {
            logger.warn("DS{} Shard {} serialization failed", dsID, shardNum);
            return Optional.empty();
        }
        Path targetDirectory = Path.of(baseDirectory.toString(), Integer.toString(versionNumber), cloudName);
        File targetDirFile = targetDirectory.toFile();
        if (!targetDirFile.exists()) {
            boolean mkdirs = targetDirFile.mkdirs();
            if (!mkdirs && !targetDirFile.exists()) {
                logger.warn("DS{} Shard {} version {} mkdirs failed: {}", dsID, shardNum, versionNumber, targetDirFile.getAbsolutePath());
                return Optional.empty();
            }
        }

        try {
            ProcessBuilder copier = new ProcessBuilder("src/main/resources/copy_shard.sh", String.format("%s/*", shardDirectory.get().toString()),
                    targetDirectory.toString());
            logger.info("Copy Command: {}", copier.command());
            Process writerProcess = copier.inheritIO().start();
            writerProcess.waitFor();
        } catch (InterruptedException | IOException e) {
            logger.warn("DS{} Shard {} version {} copy failed: {}", dsID, shardNum, versionNumber, targetDirFile.getAbsolutePath());
            return Optional.empty();
        }

        Optional<S> retShard = shardFactory.createShardFromDir(targetDirectory, shardNum);
        logger.info("DS{} Shard {}-{} copy succeeded. Time: {}ms", dsID, shardNum, versionNumber, System.currentTimeMillis() - copyStart);
        return retShard;
    }

    ManagedChannel getChannelForDSID(int dsID) {
        if (!dsIDToChannelMap.containsKey(dsID)) {
            DataStoreDescription d = zkCurator.getDSDescription(dsID);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(d.host, d.port).usePlaintext().build();
            if (dsIDToChannelMap.putIfAbsent(dsID, channel) != null) {
                channel.shutdown();
            }
        }
        return dsIDToChannelMap.get(dsID);
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

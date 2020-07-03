package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
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

public class DataStore<R extends Row, S extends Shard> {

    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);

    // Datastore metadata
    public int dsID;
    private int cloudID;
    private final String dsHost;
    private final int dsPort;
    public boolean serving = false;

    // Map from primary shard number to shard data structure.
    public final Map<Integer, S> primaryShardMap = new ConcurrentHashMap<>(); // Public for testing.
    // Map from replica shard number to shard data structure.
    public final Map<Integer, S> replicaShardMap = new ConcurrentHashMap<>();
    // Map from shard number to shard version number.
    final Map<Integer, Integer> shardVersionMap = new ConcurrentHashMap<>();
    // Map from primary shard number to last uploaded version number.
    final Map<Integer, Integer> lastUploadedVersionMap = new ConcurrentHashMap<>();
    // Map from shard number to access lock.
    final Map<Integer, ShardLock> shardLockMap = new ConcurrentHashMap<>();
    // Map from shard number to maps from version number to write query and data.
    final Map<Integer, Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>>> writeLog = new ConcurrentHashMap<>();
    // Map from primary shard number to list of replica descriptions for that shard.
    final Map<Integer, List<ReplicaDescription>> replicaDescriptionsMap = new ConcurrentHashMap<>();
    // Map from primary shard number to last timestamp known for the shard.
    final Map<Integer, Long> shardTimestampMap = new ConcurrentHashMap<>();
    // Map from Unix second timestamp to number of read queries made during that timestamp, per shard.
    final Map<Integer, Map<Long, Integer>> QPSMap = new ConcurrentHashMap<>();
    // Map from tuples of name and shard to materialized view.
    final Map<Integer, Map<String, MaterializedView>> materializedViewMap = new ConcurrentHashMap<>();
    // Map from table names to IDs.
    private final Map<String, Integer> tableIDMap = new ConcurrentHashMap<>();
    // Maximum number of shards in each table.
    private final Map<String, Integer> tableNumShardsMap = new ConcurrentHashMap<>();

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

    public static int qpsReportTimeInterval = 60; // In seconds -- should be same as load balancer interval.

    public boolean runPingDaemon = true; // Public for testing
    private final PingDaemon pingDaemon;
    private final static int pingDaemonSleepDurationMillis = 100;
    private final static int pingDaemonRefreshInterval = 10;

    // Collect execution times of all read queries.
    public final Collection<Long> readQueryExecuteTimes = new ConcurrentLinkedQueue<>();
    public final Collection<Long> readQueryFullTimes = new ConcurrentLinkedQueue<>();

    public static final int COLLECT = 0;
    public static final int PREPARE = 1;
    public static final int COMMIT = 2;
    public static final int ABORT = 3;

    public DataStore(DataStoreCloud dsCloud, ShardFactory<S> shardFactory, Path baseDirectory, String zkHost, int zkPort, String dsHost, int dsPort, int cloudID) {
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
        this.cloudID = cloudID;
        uploadShardDaemon = new UploadShardDaemon();
        pingDaemon = new PingDaemon();
    }

    /** Start serving requests. */
    public int startServing() {
        // Start serving.
        assert(!serving);
        serving = true;
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
        RegisterDataStoreMessage m = RegisterDataStoreMessage.newBuilder()
                .setHost(dsHost).setPort(dsPort).setCloudID(cloudID).build();
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
        if (!serving) {
            return;
        }
        serving = false;
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
                rd.channel.shutdownNow();
            }
        }
        coordinatorChannel.shutdownNow();
        for (Map.Entry<Integer, S> entry: primaryShardMap.entrySet()) {
            entry.getValue().destroy();
            primaryShardMap.remove(entry.getKey());
        }
        for (Map.Entry<Integer, S> entry: replicaShardMap.entrySet()) {
            entry.getValue().destroy();
            replicaShardMap.remove(entry.getKey());
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

    public void serializeMaterializedViews(int shardNum, Path dir) throws IOException {
        Path mvFile = Path.of(dir.toString(), "__uniserve__mv.obj");
        FileOutputStream f = new FileOutputStream(mvFile.toFile());
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(new Pair<>(materializedViewMap.get(shardNum), shardTimestampMap.get(shardNum)));
        o.close();
        f.close();
    }

    /** Synchronously upload a shard to the cloud, returning its name and version number. **/
    /** Assumes read lock is held **/
    // TODO:  Safely delete old versions.
    public void uploadShardToCloud(int shardNum) {
        long uploadStart = System.currentTimeMillis();
        Integer versionNumber = shardVersionMap.get(shardNum);
        if (lastUploadedVersionMap.get(shardNum).equals(versionNumber)) {
            return;
        }
        Shard shard = primaryShardMap.get(shardNum);
        // Load the shard's data into files.
        Optional<Path> shardDirectory = shard.shardToData();
        if (shardDirectory.isEmpty()) {
            logger.warn("DS{} Shard {} serialization failed", dsID, shardNum);
            return;
        }
        try {
            serializeMaterializedViews(shardNum, shardDirectory.get());
        } catch (IOException e) {
            logger.warn("DS{} Shard {} MV serialization failed", dsID, shardNum);
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
        logger.warn("DS{} Shard {}-{} upload succeeded. Time: {}ms", dsID, shardNum, versionNumber, System.currentTimeMillis() - uploadStart);
    }

    public void deserializeMaterializedViews(int shardNum, Path dir) throws IOException, ClassNotFoundException {
        Path mvFile = Path.of(dir.toString(), "__uniserve__mv.obj");
        FileInputStream f = new FileInputStream(mvFile.toFile());
        ObjectInputStream o = new ObjectInputStream(f);
        Pair<ConcurrentHashMap<String, MaterializedView>, Long> mv = (Pair<ConcurrentHashMap<String, MaterializedView>, Long>) o.readObject();
        o.close();
        f.close();
        materializedViewMap.put(shardNum, mv.getValue0());
        shardTimestampMap.put(shardNum, mv.getValue1());
    }

    /** Synchronously download a shard from the cloud **/
    public Optional<S> downloadShardFromCloud(int shardNum, String cloudName, int versionNumber, boolean materializedViews) {
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
        Optional<S> shard = shardFactory.createShardFromDir(targetDirectory, shardNum);
        if (shard.isPresent() && materializedViews) {
            try {
                deserializeMaterializedViews(shardNum, targetDirectory);
            } catch (IOException | ClassNotFoundException e) {
                logger.warn("DS{} Shard {} MV deserialization failed", dsID, shardNum);
                shard.get().destroy();
                return Optional.empty();
            }
        }
        return shard;
    }

    Pair<Integer, Integer> getTableInfo(String tableName) {
        if (tableIDMap.containsKey(tableName)) {
            return new Pair<>(tableIDMap.get(tableName), tableNumShardsMap.get(tableName));
        } else {
            DTableIDResponse r = coordinatorStub.
                    tableID(DTableIDMessage.newBuilder().setTableName(tableName).build());
            assert(r.getReturnCode() == Broker.QUERY_SUCCESS);
            int tableID = r.getId();
            int numShards = r.getNumShards();
            tableNumShardsMap.put(tableName, numShards);
            tableIDMap.put(tableName, tableID);
            return new Pair<>(tableID, numShards);
        }
    }

    private class UploadShardDaemon extends Thread {
        @Override
        public void run() {
            while (runUploadShardDaemon) {
                List<Thread> uploadThreadList = new ArrayList<>();
                for (Integer shardNum : primaryShardMap.keySet()) {
                    Thread uploadThread = new Thread(() -> {
                        shardLockMap.get(shardNum).writerLockLock(-1);
                        if (primaryShardMap.containsKey(shardNum)) {
                            uploadShardToCloud(shardNum);
                        }
                        shardLockMap.get(shardNum).writerLockUnlock();
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

package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Broker {

    private final QueryEngine queryEngine;
    private final BrokerCurator zkCurator;

    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    // Consistent hash assigning shards to servers.
    private ConsistentHash consistentHash;
    // Map from dsIDs to channels.
    private Map<Integer, ManagedChannel> dsIDToChannelMap = new ConcurrentHashMap<>();
    // Stub for communication with the coordinator.
    private BrokerCoordinatorGrpc.BrokerCoordinatorBlockingStub coordinatorBlockingStub;
    // Map from table names to IDs.
    private final Map<String, TableInfo> tableInfoMap = new ConcurrentHashMap<>();

    private final ShardMapUpdateDaemon shardMapUpdateDaemon;
    public boolean runShardMapUpdateDaemon = true;
    public static int shardMapDaemonSleepDurationMillis = 1000;

    private final QueryStatisticsDaemon queryStatisticsDaemon;
    public boolean runQueryStatisticsDaemon = true;
    public static int queryStatisticsDaemonSleepDurationMillis = 10000;

    public final Collection<Long> remoteExecutionTimes = new ConcurrentLinkedQueue<>();
    public final Collection<Long> aggregationTimes = new ConcurrentLinkedQueue<>();

    public static final int QUERY_SUCCESS = 0;
    public static final int QUERY_FAILURE = 1;
    public static final int QUERY_RETRY = 2;

    public static final int SHARDS_PER_TABLE = 1000000;

    public ConcurrentHashMap<Set<Integer>, Integer> queryStatistics = new ConcurrentHashMap<>();

    ExecutorService readQueryThreadPool = Executors.newFixedThreadPool(256);  //TODO:  Replace with async calls.

    AtomicLong txIDs = new AtomicLong(0); // TODO:  Put in ZooKeeper.


    /*
     * CONSTRUCTOR/TEARDOWN
     */

    public Broker(String zkHost, int zkPort, QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
        this.zkCurator = new BrokerCurator(zkHost, zkPort);
        Optional<Pair<String, Integer>> masterHostPort = zkCurator.getMasterLocation();
        String masterHost = null;
        Integer masterPort = null;
        if (masterHostPort.isPresent()) {
            masterHost = masterHostPort.get().getValue0();
            masterPort = masterHostPort.get().getValue1();
        } else {
            logger.error("Broker could not find master"); // TODO:  Retry.
        }
        ManagedChannel channel = ManagedChannelBuilder.forAddress(masterHost, masterPort).usePlaintext().build();
        coordinatorBlockingStub = BrokerCoordinatorGrpc.newBlockingStub(channel);
        consistentHash = zkCurator.getConsistentHashFunction();
        shardMapUpdateDaemon = new ShardMapUpdateDaemon();
        shardMapUpdateDaemon.start();
        queryStatisticsDaemon = new QueryStatisticsDaemon();
        queryStatisticsDaemon.start();
    }

    public void shutdown() {
        runShardMapUpdateDaemon = false;
        runQueryStatisticsDaemon = false;
        try {
            shardMapUpdateDaemon.join();
            queryStatisticsDaemon.interrupt();
            queryStatisticsDaemon.join();
        } catch (InterruptedException ignored) {}
        // TODO:  Synchronize with outstanding queries?
        ((ManagedChannel) this.coordinatorBlockingStub.getChannel()).shutdownNow();
        for (ManagedChannel c: dsIDToChannelMap.values()) {
            c.shutdownNow();
        }
        int numQueries = remoteExecutionTimes.size();
        if (numQueries > 0) {
            long p50RE = remoteExecutionTimes.stream().mapToLong(i -> i).sorted().toArray()[remoteExecutionTimes.size() / 2];
            long p99RE = remoteExecutionTimes.stream().mapToLong(i -> i).sorted().toArray()[remoteExecutionTimes.size() * 99 / 100];
            long p50agg = aggregationTimes.stream().mapToLong(i -> i).sorted().toArray()[aggregationTimes.size() / 2];
            long p99agg = aggregationTimes.stream().mapToLong(i -> i).sorted().toArray()[aggregationTimes.size() * 99 / 100];
            logger.info("Queries: {} p50 Remote: {}μs p99 Remote: {}μs  p50 Aggregation: {}μs p99 Aggregation: {}μs", numQueries, p50RE, p99RE, p50agg, p99agg);
        }
        zkCurator.close();
        readQueryThreadPool.shutdown();
    }

    /*
     * PUBLIC FUNCTIONS
     */

    public boolean createTable(String tableName, int numShards) {
        CreateTableMessage m = CreateTableMessage.newBuilder().setTableName(tableName).setNumShards(numShards).build();
        CreateTableResponse r = coordinatorBlockingStub.createTable(m);
        return r.getReturnCode() == QUERY_SUCCESS;
    }

    public <R extends Row, S extends Shard> boolean writeQuery(WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
        zkCurator.acquireWriteLock(); // TODO: Maybe acquire later?
        Map<Integer, List<R>> shardRowListMap = new HashMap<>();
        TableInfo tableInfo = getTableInfo(writeQueryPlan.getQueriedTable());
        for (R row: rows) {
            int partitionKey = row.getPartitionKey();
            assert(partitionKey >= 0);
            int shard = keyToShard(tableInfo.id, tableInfo.numShards, partitionKey);
            shardRowListMap.computeIfAbsent(shard, (k -> new ArrayList<>())).add(row);
        }
        Map<Integer, R[]> shardRowArrayMap = shardRowListMap.entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray((R[]) new Row[0])));
        List<WriteQueryThread<R, S>> writeQueryThreads = new ArrayList<>();
        long txID = txIDs.getAndIncrement();
        CountDownLatch queryLatch = new CountDownLatch(shardRowArrayMap.size());
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        AtomicBoolean statusWritten = new AtomicBoolean(false);
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            R[] rowArray = shardRowArrayMap.get(shardNum);
            WriteQueryThread<R, S> t = new WriteQueryThread<>(shardNum, writeQueryPlan, rowArray, txID, queryLatch, queryStatus, statusWritten);
            t.start();
            writeQueryThreads.add(t);
        }
        for (WriteQueryThread<R, S> t: writeQueryThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Write query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        assert (queryStatus.get() != QUERY_RETRY);
        zkCurator.releaseWriteLock();
        return queryStatus.get() == QUERY_SUCCESS;
    }

    public <S extends Shard, V> V anchoredReadQuery(AnchoredReadQueryPlan<S, V> plan) {
        long txID = txIDs.getAndIncrement();
        Map<String, List<Integer>> partitionKeys = plan.keysForQuery();
        HashMap<String, List<Integer>> targetShards = new HashMap<>();
        for(Map.Entry<String, List<Integer>> entry: partitionKeys.entrySet()) {
            String tableName = entry.getKey();
            List<Integer> tablePartitionKeys = entry.getValue();
            TableInfo tableInfo = getTableInfo(tableName);
            int tableID = tableInfo.id;
            int numShards = tableInfo.numShards;
            List<Integer> shardNums;
            if (tablePartitionKeys.contains(-1)) {
                // -1 is a wildcard--run on all shards.
                shardNums = IntStream.range(tableID * SHARDS_PER_TABLE, tableID * SHARDS_PER_TABLE + numShards)
                        .boxed().collect(Collectors.toList());
            } else {
                shardNums = tablePartitionKeys.stream().map(i -> keyToShard(tableID, numShards, i))
                        .distinct().collect(Collectors.toList());
            }
            targetShards.put(tableName, shardNums);
        }
        ByteString serializedTargetShards = Utilities.objectToByteString(targetShards);
        ByteString serializedQuery = Utilities.objectToByteString(plan);
        String anchorTable = plan.getAnchorTable();
        List<Integer> anchorTableShards = targetShards.get(anchorTable);
        int numReducers = anchorTableShards.size();
        List<ByteString> intermediates = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(numReducers);
        for (int anchorShardNum: anchorTableShards) {
            int dsID = consistentHash.getBucket(anchorShardNum);
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            AnchoredReadQueryMessage m = AnchoredReadQueryMessage.newBuilder().
                    setTargetShard(anchorShardNum).setSerializedQuery(serializedQuery).setNumReducers(numReducers)
                    .setTxID(txID).setTargetShards(serializedTargetShards).build();
            StreamObserver<AnchoredReadQueryResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(AnchoredReadQueryResponse r) {
                    assert(r.getReturnCode() == Broker.QUERY_SUCCESS);
                    intermediates.add(r.getResponse());
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("Read Query Error on DS{}: {}", dsID, throwable.getMessage());
                    try {
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                    } catch (InterruptedException ignored) {}
                    int newDSID = consistentHash.getBucket(anchorShardNum);
                    ManagedChannel channel = dsIDToChannelMap.get(newDSID);
                    BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
                    stub.anchoredReadQuery(m, this);
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };
            stub.anchoredReadQuery(m, responseObserver);
        }
        try {
            latch.await();
        } catch (InterruptedException ignored) { }
        long aggStart = System.nanoTime();
        V ret =  plan.aggregateShardQueries(intermediates);
        long aggEnd = System.nanoTime();
        aggregationTimes.add((aggEnd - aggStart) / 1000L);
        return ret;
    }

    public <S extends Shard, V> V shuffleReadQuery(ShuffleReadQueryPlan<S, V> plan) {
        long txID = txIDs.getAndIncrement();
        Map<String, List<Integer>> partitionKeys = plan.keysForQuery();
        HashMap<String, List<Integer>> targetShards = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : partitionKeys.entrySet()) {
            String tableName = entry.getKey();
            List<Integer> tablePartitionKeys = entry.getValue();
            TableInfo tableInfo = getTableInfo(tableName);
            int tableID = tableInfo.id;
            int numShards = tableInfo.numShards;
            List<Integer> shardNums;
            if (tablePartitionKeys.contains(-1)) {
                // -1 is a wildcard--run on all shards.
                shardNums = IntStream.range(tableID * SHARDS_PER_TABLE, tableID * SHARDS_PER_TABLE + numShards)
                        .boxed().collect(Collectors.toList());
            } else {
                shardNums = tablePartitionKeys.stream().map(i -> keyToShard(tableID, numShards, i))
                        .distinct().collect(Collectors.toList());
            }
            targetShards.put(tableName, shardNums);
        }
        ByteString serializedTargetShards = Utilities.objectToByteString(targetShards);
        ByteString serializedQuery = Utilities.objectToByteString(plan);
        Set<Integer> dsIDs = dsIDToChannelMap.keySet();
        int numReducers = dsIDs.size();
        List<ByteString> intermediates = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(numReducers);
        int reducerNum = 0;
        for (int dsID : dsIDs) {
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            ShuffleReadQueryMessage m = ShuffleReadQueryMessage.newBuilder().
                    setReducerNum(reducerNum).setSerializedQuery(serializedQuery).setNumReducers(numReducers)
                    .setTxID(txID).setTargetShards(serializedTargetShards).build();
            reducerNum++;
            StreamObserver<ShuffleReadQueryResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(ShuffleReadQueryResponse r) {
                    assert (r.getReturnCode() == Broker.QUERY_SUCCESS);
                    intermediates.add(r.getResponse());
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("Read Query Error on DS{}: {}", dsID, throwable.getMessage());
                    int numDSIDs = dsIDToChannelMap.keySet().size();
                    Integer newDSID = dsIDToChannelMap.keySet().stream().skip(new Random().nextInt(numDSIDs)).findFirst().orElse(null);
                    ManagedChannel channel = dsIDToChannelMap.get(newDSID);
                    BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
                    stub.shuffleReadQuery(m, this);
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };
            stub.shuffleReadQuery(m, responseObserver);
        }
        try {
            latch.await();
        } catch (InterruptedException ignored) {
        }
        long aggStart = System.nanoTime();
        V ret = plan.aggregateShardQueries(intermediates);
        long aggEnd = System.nanoTime();
        aggregationTimes.add((aggEnd - aggStart) / 1000L);
        return ret;
    }

    public <S extends Shard, V> boolean registerMaterializedView(AnchoredReadQueryPlan<S, V> readQueryPlan, String name) {
        List<Integer> partitionKeys = readQueryPlan.keysForQuery().get(readQueryPlan.getQueriedTables().get(0));
        List<Integer> shardNums;
        TableInfo tableInfo = getTableInfo(readQueryPlan.getQueriedTables().get(0));
        int tableID = tableInfo.id;
        int numShards = tableInfo.numShards;
        if (partitionKeys.contains(-1)) {
            // -1 is a wildcard--run on all shards.
            shardNums = IntStream.range(tableID * SHARDS_PER_TABLE, tableID * SHARDS_PER_TABLE + numShards).boxed().collect(Collectors.toList());
        } else {
            shardNums = partitionKeys.stream().map(i -> keyToShard(tableID, numShards, i)).distinct().collect(Collectors.toList());
        }
        for (int shardNum: shardNums) {
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = getStubForShard(shardNum);
            ByteString serializedQuery = Utilities.objectToByteString(readQueryPlan);
            RegisterMaterializedViewMessage m = RegisterMaterializedViewMessage.newBuilder().
                    setShard(shardNum).setName(name).setSerializedQuery(serializedQuery).build();
            RegisterMaterializedViewResponse r = stub.registerMaterializedView(m);
            if (r.getReturnCode() != Broker.QUERY_SUCCESS) {
                // TODO:  Handle retries and do full rollbacks on failure.
                return false;
            }
        }
        return true;
    }

    public <S extends Shard, V> V queryMaterializedView(AnchoredReadQueryPlan<S, V> readQueryPlan, String name) {
        List<Integer> partitionKeys = readQueryPlan.keysForQuery().get(readQueryPlan.getQueriedTables().get(0));
        List<Integer> shardNums;
        TableInfo tableInfo = getTableInfo(readQueryPlan.getQueriedTables().get(0));
        int tableID = tableInfo.id;
        int numShards = tableInfo.numShards;
        if (partitionKeys.contains(-1)) {
            // -1 is a wildcard--run on all shards.
            shardNums = IntStream.range(tableID * SHARDS_PER_TABLE, tableID * SHARDS_PER_TABLE + numShards).boxed().collect(Collectors.toList());
        } else {
            shardNums = partitionKeys.stream().map(i -> keyToShard(tableID, numShards, i)).distinct().collect(Collectors.toList());
        }
        List<ByteString> intermediates = new ArrayList<>();
        for (int shardNum: shardNums) {
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = getStubForShard(shardNum);
            QueryMaterializedViewMessage m = QueryMaterializedViewMessage.newBuilder().
                    setShard(shardNum).setName(name).build();
            QueryMaterializedViewResponse r = stub.queryMaterializedView(m);
            assert r.getReturnCode() == Broker.QUERY_SUCCESS; // TODO:  Handle retries and failures.
            intermediates.add(r.getResponse());
        }
        return readQueryPlan.aggregateShardQueries(intermediates);
    }

    /*
     * PRIVATE FUNCTIONS
     */

    private TableInfo getTableInfo(String tableName) {
        if (tableInfoMap.containsKey(tableName)) {
            return tableInfoMap.get(tableName);
        } else {
            TableInfoResponse r = coordinatorBlockingStub.
                    tableInfo(TableInfoMessage.newBuilder().setTableName(tableName).build());
            assert(r.getReturnCode() == QUERY_SUCCESS);
            TableInfo t = new TableInfo(tableName, r.getId(), r.getNumShards());
            tableInfoMap.put(tableName, t);
            return t;
        }
    }

    private static int keyToShard(int tableID, int numShards, int partitionKey) {
        return tableID * SHARDS_PER_TABLE + (partitionKey % numShards);
    }

    private BrokerDataStoreGrpc.BrokerDataStoreBlockingStub getStubForShard(int shard) {
        int dsID = consistentHash.getBucket(shard);
        ManagedChannel channel = dsIDToChannelMap.get(dsID);
        assert(channel != null);
        return BrokerDataStoreGrpc.newBlockingStub(channel);
    }

    public void sendStatisticsToCoordinator() {
        ByteString queryStatisticsSer = Utilities.objectToByteString(queryStatistics);
        QueryStatisticsMessage m = QueryStatisticsMessage.newBuilder().setQueryStatistics(queryStatisticsSer).build();
        QueryStatisticsResponse r = coordinatorBlockingStub.queryStatistics(m);
    }

    private class QueryStatisticsDaemon extends Thread {
        @Override
        public void run() {
            while (runQueryStatisticsDaemon) {
                sendStatisticsToCoordinator();
                queryStatistics = new ConcurrentHashMap<>();
                try {
                    Thread.sleep(queryStatisticsDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class ShardMapUpdateDaemon extends Thread {

        private void updateMap() {
            ConsistentHash consistentHash = zkCurator.getConsistentHashFunction();
            Map<Integer, ManagedChannel> dsIDToChannelMap = new HashMap<>();
            int dsID = 0;
            while (true) {
                DataStoreDescription d = zkCurator.getDSDescriptionFromDSID(dsID);
                if (d == null) {
                    break;
                } else if (d.status.get() == DataStoreDescription.ALIVE) {
                    ManagedChannel channel = Broker.this.dsIDToChannelMap.containsKey(dsID) ?
                            Broker.this.dsIDToChannelMap.get(dsID) :
                            ManagedChannelBuilder.forAddress(d.host, d.port).usePlaintext().build();
                    dsIDToChannelMap.put(dsID, channel);
                } else if (d.status.get() == DataStoreDescription.DEAD) {
                    if (Broker.this.dsIDToChannelMap.containsKey(dsID)) {
                        Broker.this.dsIDToChannelMap.get(dsID).shutdown();
                    }
                }
                dsID++;
            }
            Broker.this.dsIDToChannelMap = dsIDToChannelMap;
            Broker.this.consistentHash = consistentHash;
        }

        @Override
        public void run() {
            while (runShardMapUpdateDaemon) {
                updateMap();
                try {
                    Thread.sleep(shardMapDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        @Override
        public synchronized void start() {
            updateMap();
            super.start();
        }
    }

    private class WriteQueryThread<R extends Row, S extends Shard> extends Thread {
        private final int shardNum;
        private final WriteQueryPlan<R, S> writeQueryPlan;
        private final R[] rowArray;
        private final long txID;
        private CountDownLatch queryLatch;
        private AtomicInteger queryStatus;
        private AtomicBoolean statusWritten;

        WriteQueryThread(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, R[] rowArray, long txID,
                         CountDownLatch queryLatch, AtomicInteger queryStatus, AtomicBoolean statusWritten) {
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.rowArray = rowArray;
            this.txID = txID;
            this.queryLatch = queryLatch;
            this.queryStatus = queryStatus;
            this.statusWritten = statusWritten;
        }

        @Override
        public void run() { writeQuery(); }

        private void writeQuery() {
            AtomicInteger subQueryStatus = new AtomicInteger(QUERY_RETRY);
            while (subQueryStatus.get() == QUERY_RETRY) {
                BrokerDataStoreGrpc.BrokerDataStoreBlockingStub blockingStub = getStubForShard(shardNum);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(blockingStub.getChannel());
                final CountDownLatch prepareLatch = new CountDownLatch(1);
                final CountDownLatch finishLatch = new CountDownLatch(1);
                StreamObserver<WriteQueryMessage> observer =
                        stub.writeQuery(new StreamObserver<>() {
                            @Override
                            public void onNext(WriteQueryResponse writeQueryResponse) {
                                subQueryStatus.set(writeQueryResponse.getReturnCode());
                                prepareLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable th) {
                                logger.warn("Write query RPC failed for shard {}", shardNum);
                                subQueryStatus.set(QUERY_FAILURE);
                                prepareLatch.countDown();
                                finishLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                finishLatch.countDown();
                            }
                        });
                final int STEPSIZE = 1000;
                for (int i = 0; i < rowArray.length; i += STEPSIZE) {
                    ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                    R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    WriteQueryMessage rowMessage = WriteQueryMessage.newBuilder()
                            .setShard(shardNum).
                            setSerializedQuery(serializedQuery)
                            .setRowData(rowData)
                            .setTxID(txID)
                            .setWriteState(DataStore.COLLECT)
                            .build();
                    observer.onNext(rowMessage);
                }
                WriteQueryMessage prepare = WriteQueryMessage.newBuilder()
                        .setWriteState(DataStore.PREPARE)
                        .build();
                observer.onNext(prepare);
                try {
                    prepareLatch.await();
                } catch (InterruptedException e) {
                    logger.error("Write Interrupted: {}", e.getMessage());
                    assert (false);
                }
                if (subQueryStatus.get() == QUERY_RETRY) {
                    try {
                        observer.onCompleted();
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                        continue;
                    } catch (InterruptedException e) {
                        logger.error("Write Interrupted: {}", e.getMessage());
                        assert (false);
                    }
                }
                assert(subQueryStatus.get() != QUERY_RETRY);
                if (subQueryStatus.get() == QUERY_FAILURE) {
                    queryStatus.set(QUERY_FAILURE);
                }
                queryLatch.countDown();
                try {
                    queryLatch.await();
                } catch (InterruptedException e) {
                    logger.error("Write Interrupted: {}", e.getMessage());
                    assert (false);
                }
                assert(queryStatus.get() != QUERY_RETRY);
                if (queryStatus.get() == QUERY_SUCCESS) {
                    // TODO:  This must finish before any commit message is sent.
                    if (statusWritten.compareAndSet(false, true)) {
                        zkCurator.writeTransactionStatus(txID, DataStore.COMMIT);
                    }
                    WriteQueryMessage commit = WriteQueryMessage.newBuilder()
                            .setWriteState(DataStore.COMMIT)
                            .build();
                    observer.onNext(commit);
                } else if (queryStatus.get() == QUERY_FAILURE) {
                    WriteQueryMessage abort = WriteQueryMessage.newBuilder()
                            .setWriteState(DataStore.ABORT)
                            .build();
                    observer.onNext(abort);
                }
                observer.onCompleted();
                try {
                    finishLatch.await();
                } catch (InterruptedException e) {
                    logger.error("Write Interrupted: {}", e.getMessage());
                    assert (false);
                }
            }
        }
    }
}


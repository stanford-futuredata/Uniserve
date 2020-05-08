package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Broker {

    private final QueryEngine queryEngine;
    private final BrokerCurator zkCurator;

    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    // Map from dsIDs to stubs.
    private final Map<Integer, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> dsIDToStubMap = new ConcurrentHashMap<>();
    // Map from shards to the primary's DataStoreBlockingStubs.
    private final Map<Integer, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> shardToPrimaryStubMap = new ConcurrentHashMap<>();
    // Map from shards to the replicas' DataStoreBlockingStubs.
    private final Map<Integer, List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub>> shardToReplicaStubMap = new ConcurrentHashMap<>();
    // Map from shards to the replicas' ratios.
    private final Map<Integer, List<Double>> replicaShardToRatioMap = new ConcurrentHashMap<>();
    // Stub for communication with the coordinator.
    private BrokerCoordinatorGrpc.BrokerCoordinatorBlockingStub coordinatorBlockingStub;
    // Maximum number of shards.
    private static int numShards;

    private ShardMapUpdateDaemon shardMapUpdateDaemon;
    public boolean runShardMapUpdateDaemon = true;
    public static int shardMapDaemonSleepDurationMillis = 1000;

    private QueryStatisticsDaemon queryStatisticsDaemon;
    public boolean runQueryStatisticsDaemon = true;
    public static int queryStatisticsDaemonSleepDurationMillis = 10000;

    public final Collection<Long> remoteExecutionTimes = new ConcurrentLinkedQueue<>();
    public final Collection<Long> aggregationTimes = new ConcurrentLinkedQueue<>();

    public static final int QUERY_SUCCESS = 0;
    public static final int QUERY_FAILURE = 1;
    public static final int QUERY_RETRY = 2;

    public ConcurrentHashMap<Set<Integer>, Integer> queryStatistics = new ConcurrentHashMap<>();

    ExecutorService readQueryThreadPool = Executors.newFixedThreadPool(256);  //TODO:  Replace with async calls.


    /*
     * CONSTRUCTOR/TEARDOWN
     */

    public Broker(String zkHost, int zkPort, QueryEngine queryEngine, int numShards) {
        this.queryEngine = queryEngine;
        Broker.numShards = numShards;
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
        shardMapUpdateDaemon = new ShardMapUpdateDaemon();
        shardMapUpdateDaemon.start();
        queryStatisticsDaemon = new QueryStatisticsDaemon();
        queryStatisticsDaemon.start();
    }

    public void shutdown() {
        runShardMapUpdateDaemon = false;
        runQueryStatisticsDaemon = false;
        try {
            shardMapUpdateDaemon.interrupt();
            shardMapUpdateDaemon.join();
            queryStatisticsDaemon.interrupt();
            queryStatisticsDaemon.join();
        } catch (InterruptedException ignored) {}
        // TODO:  Synchronize with outstanding queries?
        ((ManagedChannel) this.coordinatorBlockingStub.getChannel()).shutdown();
        for (BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub: dsIDToStubMap.values()) {
            ((ManagedChannel) stub.getChannel()).shutdown();
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

    public <R extends Row, S extends Shard> boolean writeQuery(WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
        Map<Integer, List<R>> shardRowListMap = new HashMap<>();
        for (R row: rows) {
            int partitionKey = row.getPartitionKey();
            assert(partitionKey >= 0);
            int shard = keyToShard(partitionKey);
            shardRowListMap.computeIfAbsent(shard, (k -> new ArrayList<>())).add(row);
        }
        Map<Integer, R[]> shardRowArrayMap = shardRowListMap.entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray((R[]) new Row[0])));
        List<WriteQueryThread<R, S>> writeQueryThreads = new ArrayList<>();
        long txID = ThreadLocalRandom.current().nextLong();
        CountDownLatch queryLatch = new CountDownLatch(shardRowArrayMap.size());
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            R[] rowArray = shardRowArrayMap.get(shardNum);
            WriteQueryThread<R, S> t = new WriteQueryThread<>(shardNum, writeQueryPlan, rowArray, txID, queryLatch, queryStatus);
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
        return queryStatus.get() == QUERY_SUCCESS;
    }

    public <S extends Shard, V> V readQuery(ReadQueryPlan<S, V> readQueryPlan) {
        // If there aren't subqueries, execute the query directly.
        if (readQueryPlan.getSubQueries().isEmpty()) {
            return executeReadQueryStage(readQueryPlan);
        }
        Set<ReadQueryPlan> unexecutedReadQueryPlans = new HashSet<>();
        // DFS the query plan tree to construct a list of all sub-query plans.
        Stack<ReadQueryPlan> searchStack = new Stack<>();
        searchStack.push(readQueryPlan);
        while (!searchStack.empty()) {
            ReadQueryPlan q = searchStack.pop();
            unexecutedReadQueryPlans.add(q);
            List<ReadQueryPlan> subQueries = q.getSubQueries();
            for (ReadQueryPlan sq : subQueries) {
                searchStack.push(sq);
            }
        }
        // Step through the list of plans, executing them sequentially in dependency order.
        Map<ReadQueryPlan, Object> executedQueryPlans = new HashMap<>();
        while (!unexecutedReadQueryPlans.isEmpty()) {
            Set<ReadQueryPlan> updatedUnexecutedReadQueryPlans = new HashSet<>();
            for (ReadQueryPlan q: unexecutedReadQueryPlans) {
                if (executedQueryPlans.keySet().containsAll(q.getSubQueries())) {
                    List<Object> subQueryResults = new ArrayList<>();
                    List<ReadQueryPlan> subQueries = q.getSubQueries();
                    for (ReadQueryPlan sq: subQueries) {
                        subQueryResults.add(executedQueryPlans.get(sq));
                    }
                    q.setSubQueryResults(subQueryResults);
                    Object o = executeReadQueryStage(q);
                    executedQueryPlans.put(q, o);
                } else {
                    updatedUnexecutedReadQueryPlans.add(q);
                }
            }
            unexecutedReadQueryPlans = updatedUnexecutedReadQueryPlans;
        }
        return (V) executedQueryPlans.get(readQueryPlan);
    }

    /*
     * PRIVATE FUNCTIONS
     */

    private static int keyToShard(int partitionKey) {
        return partitionKey % Broker.numShards;
    }

    private BrokerDataStoreGrpc.BrokerDataStoreBlockingStub createDataStoreStub(DataStoreDescription dsDescription) {
        BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = dsIDToStubMap.getOrDefault(dsDescription.dsID, null);
        if (stub == null) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(dsDescription.host, dsDescription.port).usePlaintext().build();
            stub = dsIDToStubMap.putIfAbsent(dsDescription.dsID, BrokerDataStoreGrpc.newBlockingStub(channel));
            if (stub == null) {
                stub = dsIDToStubMap.get(dsDescription.dsID); // No entry exists.
            } else {
                channel.shutdown(); // Stub already exists.
            }
        }
        return stub;
    }

    private Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> getPrimaryStubForShard(int shard) {
        // First, check the local shard-to-server map.
        BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = shardToPrimaryStubMap.getOrDefault(shard, null);
        if (stub == null) {
            // TODO:  This is thread-safe, but might make many redundant requests.
            DataStoreDescription dsDescription;
            // Then, try to pull it from ZooKeeper.
            Optional<DataStoreDescription> dsDescriptionOpt = zkCurator.getShardPrimaryDSDescription(shard);
            if (dsDescriptionOpt.isPresent()) {
                dsDescription = dsDescriptionOpt.get();
            } else {
                // Otherwise, ask the coordinator.
                ShardLocationMessage m = ShardLocationMessage.newBuilder().setShard(shard).build();
                ShardLocationResponse r;
                try {
                    r = coordinatorBlockingStub.shardLocation(m);
                } catch (StatusRuntimeException e) {
                    logger.warn("RPC failed: {}", e.getStatus());
                    return Optional.empty();
                }
                dsDescription = new DataStoreDescription(r.getDsID(), DataStoreDescription.ALIVE, r.getHost(), r.getPort());
            }
            stub = createDataStoreStub(dsDescription);
            shardToPrimaryStubMap.putIfAbsent(shard, stub);
            stub = shardToPrimaryStubMap.get(shard);
        }
        return Optional.of(stub);
    }

    private Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> getAnyStubForShard(int shard) {
        // If replicas are known, return a random replica.
        List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubs = shardToReplicaStubMap.getOrDefault(shard, null);
        if (stubs != null && stubs.size() > 0) {
            // If replicas exist, randomly choose a replica with probability equal to its ratio.
            List<Double> ratios = replicaShardToRatioMap.get(shard);
            assert(ratios.size() == stubs.size());
            // Sum of ratios is <= 1.0, if no replica is chosen then read from the primary.
            assert(ratios.stream().mapToDouble(i -> i).sum() <= 1.0);
            double r = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
            double countWeight = 0.0;
            for (int i = 0; i < ratios.size(); i++) {
                countWeight += ratios.get(i);
                if (countWeight >= r) {
                    return Optional.of(stubs.get(i));
                }
            }
        }
        // Otherwise, return the primary if it is known.
        return getPrimaryStubForShard(shard);
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
        @Override
        public void run() {
            while (runShardMapUpdateDaemon) {
                for (Integer shardNum : shardToPrimaryStubMap.keySet()) {
                    Optional<DataStoreDescription> primaryDSDescriptionsOpt =
                            zkCurator.getShardPrimaryDSDescription(shardNum);
                    Optional<Pair<List<DataStoreDescription>, List<Double>>> replicaDSDescriptionsOpt =
                            zkCurator.getShardReplicaDSDescriptions(shardNum);
                    if (primaryDSDescriptionsOpt.isEmpty() || replicaDSDescriptionsOpt.isEmpty()) {
                        logger.error("ZK has lost information on Shard {}", shardNum);
                        continue;
                    }
                    DataStoreDescription primaryDSDescription = primaryDSDescriptionsOpt.get();
                    List<DataStoreDescription> replicaDSDescriptions = replicaDSDescriptionsOpt.get().getValue0();
                    List<Double> replicaRatios = replicaDSDescriptionsOpt.get().getValue1();
                    BrokerDataStoreGrpc.BrokerDataStoreBlockingStub primaryStub =
                            createDataStoreStub(primaryDSDescription);
                    List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> replicaStubs = new ArrayList<>();
                    for (DataStoreDescription replicaDSDescription: replicaDSDescriptions) {
                        replicaStubs.add(createDataStoreStub(replicaDSDescription));
                    }
                    shardToPrimaryStubMap.put(shardNum, primaryStub);
                    shardToReplicaStubMap.put(shardNum, replicaStubs);
                    replicaShardToRatioMap.put(shardNum, replicaRatios);
                }
                try {
                    Thread.sleep(shardMapDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class WriteQueryThread<R extends Row, S extends Shard> extends Thread {
        private final int shardNum;
        private final WriteQueryPlan<R, S> writeQueryPlan;
        private final R[] rowArray;
        private final long txID;
        private CountDownLatch queryLatch;
        private AtomicInteger queryStatus;

        WriteQueryThread(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, R[] rowArray, long txID, CountDownLatch queryLatch, AtomicInteger queryStatus) {
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.rowArray = rowArray;
            this.txID = txID;
            this.queryLatch = queryLatch;
            this.queryStatus = queryStatus;
        }

        @Override
        public void run() { writeQuery(); }

        private void writeQuery() {
            AtomicInteger subQueryStatus = new AtomicInteger(QUERY_RETRY);
            while (subQueryStatus.get() == QUERY_RETRY) {
                Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getPrimaryStubForShard(shardNum);
                if (stubOpt.isEmpty()) {
                    logger.error("Could not find DataStore for shard {}", shardNum);
                    assert(false);
                }
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(stubOpt.get().getChannel());
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
                                assert(false);
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


    private <S extends Shard, V> V executeReadQueryStage(ReadQueryPlan<S, V> readQueryPlan) {
        List<Integer> partitionKeys = readQueryPlan.keysForQuery();
        List<Integer> shardNums;
        if (partitionKeys.contains(-1)) {
            // -1 is a wildcard--run on all shards.
            shardNums = IntStream.range(0, numShards).boxed().collect(Collectors.toList());
        } else {
            shardNums = partitionKeys.stream().map(Broker::keyToShard).distinct().collect(Collectors.toList());
        }
        queryStatistics.merge(new HashSet<>(shardNums), 1, Integer::sum);
        List<ByteString> intermediates = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int shardNum : shardNums) {
            ReadQueryShardThread readQueryShardThread = new ReadQueryShardThread(shardNum, readQueryPlan, intermediates);
            Future<?> f = readQueryThreadPool.submit(readQueryShardThread);
            futures.add(f);
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        if (intermediates.contains(null)) {
            return null;
        }
        long aggStart = System.nanoTime();
        V ret =  readQueryPlan.aggregateShardQueries(intermediates);
        long aggEnd = System.nanoTime();
        aggregationTimes.add((aggEnd - aggStart) / 1000L);
        return ret;
    }

    private class ReadQueryShardThread implements Runnable {
        private final int shardNum;
        private final ReadQueryPlan readQueryPlan;
        private List<ByteString> intermediates;

        ReadQueryShardThread(int shardNum, ReadQueryPlan readQueryPlan, List<ByteString> intermediates) {
            this.shardNum = shardNum;
            this.readQueryPlan = readQueryPlan;
            this.intermediates = intermediates;
        }

        @Override
        public void run() {
            Optional<ByteString> intermediate = queryShard(this.shardNum);
            if (intermediate.isPresent()) {
                intermediates.add(intermediate.get());
            } else {
                intermediates.add(null);
            }
        }

        private Optional<ByteString> queryShard(int shard) {
            int queryStatus = QUERY_RETRY;
            ReadQueryResponse readQueryResponse = null;
            int tries = 0;
            while (queryStatus == QUERY_RETRY) {
                tries++;
                Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getAnyStubForShard(shard);
                if (stubOpt.isEmpty()) {
                    logger.warn("Could not find DataStore for shard {}", shard);
                    return Optional.empty();
                }
                BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = stubOpt.get();
                ByteString serializedQuery;
                serializedQuery = Utilities.objectToByteString(readQueryPlan);
                ReadQueryMessage readQuery = ReadQueryMessage.newBuilder().setShard(shard).setSerializedQuery(serializedQuery).build();
                try {
                    long remoteStart = System.nanoTime();
                    readQueryResponse = stub.readQuery(readQuery);
                    long remoteEnd = System.nanoTime();
                    remoteExecutionTimes.add((remoteEnd - remoteStart) / 1000L);
                    queryStatus = readQueryResponse.getReturnCode();
                    assert queryStatus != QUERY_FAILURE;
                } catch (StatusRuntimeException e) {
                    queryStatus = QUERY_RETRY;
                }
                if (queryStatus == QUERY_RETRY && tries == 0) {
                    try {
                        Thread.sleep((shardMapDaemonSleepDurationMillis * 12) / 10);
                    } catch (InterruptedException ignored) { }
                }
                if (tries > 100) {
                    logger.warn("Query timed out on shard {}", shardNum);
                    return Optional.empty();
                }
            }
            ByteString responseByteString = readQueryResponse.getResponse();
            return Optional.of(responseByteString);
        }
    }
}


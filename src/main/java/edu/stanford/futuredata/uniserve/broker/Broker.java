package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
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

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Broker {

    private final QueryEngine queryEngine;
    private final BrokerCurator zkCurator;

    private static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    // Map from dsIDs to stubs.
    private final Map<Integer, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> dsIDToStubMap = new ConcurrentHashMap<>();
    // Map from shards to the primary's DataStoreBlockingStubs.
    private final Map<Integer, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> shardToPrimaryStubMap = new ConcurrentHashMap<>();
    // Map from shards to the replicas' DataStoreBlockingStubs.
    private final Map<Integer, List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub>> shardToReplicaStubMap = new ConcurrentHashMap<>();
    // Stub for communication with the coordinator.
    private BrokerCoordinatorGrpc.BrokerCoordinatorBlockingStub coordinatorBlockingStub;
    // Maximum number of shards.
    private static int numShards;
    // Daemon thread updating the shard maps.
    private ShardMapUpdateDaemon shardMapUpdateDaemon;
    // Should the daemon run?
    private boolean runShardMapUpdateDaemon = true;
    // How long should the daemon wait between runs?
    private static final int shardMapDaemonSleepDurationMillis = 1000;

    public static final int QUERY_SUCCESS = 0;
    public static final int QUERY_FAILURE = 1;
    public static final int QUERY_RETRY = 2;


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
    }

    public void shutdown() {
        runShardMapUpdateDaemon = false;
        try {
            shardMapUpdateDaemon.interrupt();
            shardMapUpdateDaemon.join();
        } catch (InterruptedException ignored) {}
        // TODO:  Synchronize with outstanding queries?
        ((ManagedChannel) this.coordinatorBlockingStub.getChannel()).shutdown();
        for (BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub: this.shardToPrimaryStubMap.values()) {
            ((ManagedChannel) stub.getChannel()).shutdown();
        }
        for (List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubs: this.shardToReplicaStubMap.values()) {
            for (BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub: stubs) {
                ((ManagedChannel) stub.getChannel()).shutdown();
            }
        }
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
        List<WriteQueryPreCommitShardThread<R, S>> writeQueryPreCommitShardThreads = new ArrayList<>();
        long txID = ThreadLocalRandom.current().nextLong();
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            R[] rowArray = shardRowArrayMap.get(shardNum);
            WriteQueryPreCommitShardThread<R, S> t = new WriteQueryPreCommitShardThread<>(shardNum, writeQueryPlan, rowArray, txID);
            t.start();
            writeQueryPreCommitShardThreads.add(t);
        }
        boolean success = true; // Commit on true, abort on false.
        for (WriteQueryPreCommitShardThread<R, S> t: writeQueryPreCommitShardThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Write query interrupted: {}", e.getMessage());
                assert(false);
            }
            success = success && t.isSuccess();
        }
        List<WriteQueryCommitShardThread> writeQueryCommitShardThreads = new ArrayList<>();
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            WriteQueryCommitShardThread t = new WriteQueryCommitShardThread(shardNum, success, txID);
            t.start();
            writeQueryCommitShardThreads.add(t);
        }
        for (WriteQueryCommitShardThread t: writeQueryCommitShardThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Write query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        return success;
    }

    public <S extends Shard, T extends Serializable, V> V readQuery(ReadQueryPlan<S, T, V> readQueryPlan) {
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
        // Spin on the list of sub-query plans, asynchronously executing each as soon as its dependencies are resolved.
        Map<ReadQueryPlan, Object> executedQueryPlans = new HashMap<>();
        List<ExecuteReadQueryStageThread> executeReadQueryStageThreads = new ArrayList<>();
        while (!unexecutedReadQueryPlans.isEmpty()) {
            // If a subquery thread has finished, store its result and remove the thread.
            List<ExecuteReadQueryStageThread> updatedExecuteReadQueryStageThreads = new ArrayList<>();
            for (ExecuteReadQueryStageThread t : executeReadQueryStageThreads) {
                if (!t.isAlive()) {
                    executedQueryPlans.put(t.getReadQueryPlan(), t.getQueryResult());
                } else {
                    updatedExecuteReadQueryStageThreads.add(t);
                }
            }
            // If all dependencies of a subquery are resolved, execute the subquery.
            executeReadQueryStageThreads = updatedExecuteReadQueryStageThreads;
            Set<ReadQueryPlan> updatedUnexecutedReadQueryPlans = new HashSet<>();
            for (ReadQueryPlan q: unexecutedReadQueryPlans) {
                if (executedQueryPlans.keySet().containsAll(q.getSubQueries())) {
                    List<Object> subQueryResults = new ArrayList<>();
                    List<ReadQueryPlan> subQueries = q.getSubQueries();
                    for (ReadQueryPlan sq: subQueries) {
                        subQueryResults.add(executedQueryPlans.get(sq));
                    }
                    q.setSubQueryResults(subQueryResults);
                    ExecuteReadQueryStageThread t = new ExecuteReadQueryStageThread(q);
                    t.start();
                    executeReadQueryStageThreads.add(t);
                } else {
                    updatedUnexecutedReadQueryPlans.add(q);
                }
            }
            unexecutedReadQueryPlans = updatedUnexecutedReadQueryPlans;
        }
        // When all dependencies are resolved, the only query still executing will be the top-level one.  Return its result.
        assert(executeReadQueryStageThreads.size() == 1);
        ExecuteReadQueryStageThread<S, T, V> finalThread = (ExecuteReadQueryStageThread<S, T, V>) executeReadQueryStageThreads.get(0);
        try {
            finalThread.join();
        } catch (InterruptedException e) {
            logger.error("Query execution thread interrupted: {}", e.getMessage());
            assert(false);
        }
        return finalThread.getQueryResult();
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
            return Optional.of(stubs.get(ThreadLocalRandom.current().nextInt(stubs.size())));
        }
        // Otherwise, return the primary if it is known.
        return getPrimaryStubForShard(shard);
    }

    private class ShardMapUpdateDaemon extends Thread {
        @Override
        public void run() {
            while (runShardMapUpdateDaemon) {
                for (Integer shardNum : shardToPrimaryStubMap.keySet()) {
                    Optional<DataStoreDescription> primaryDSDescriptionsOpt =
                            zkCurator.getShardPrimaryDSDescription(shardNum);
                    Optional<List<DataStoreDescription>> replicaDSDescriptionsOpt =
                            zkCurator.getShardReplicaDSDescriptions(shardNum);
                    if (primaryDSDescriptionsOpt.isEmpty() || replicaDSDescriptionsOpt.isEmpty()) {
                        logger.error("ZK has lost information on Shard {}", shardNum);
                        continue;
                    }
                    DataStoreDescription primaryDSDescription = primaryDSDescriptionsOpt.get();
                    List<DataStoreDescription> replicaDSDescriptions = replicaDSDescriptionsOpt.get();
                    BrokerDataStoreGrpc.BrokerDataStoreBlockingStub primaryStub =
                            createDataStoreStub(primaryDSDescription);
                    List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> replicaStubs = new ArrayList<>();
                    for (DataStoreDescription replicaDSDescription: replicaDSDescriptions) {
                        replicaStubs.add(createDataStoreStub(replicaDSDescription));
                    }
                    shardToPrimaryStubMap.put(shardNum, primaryStub);
                    shardToReplicaStubMap.put(shardNum, replicaStubs);
                }
                try {
                    Thread.sleep(shardMapDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class WriteQueryPreCommitShardThread<R extends Row, S extends Shard> extends Thread {
        private final int shardNum;
        private final WriteQueryPlan<R, S> writeQueryPlan;
        private final R[] rowArray;
        private final long txID;
        private boolean success;

        WriteQueryPreCommitShardThread(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, R[] rowArray, long txID) {
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.rowArray = rowArray;
            this.txID = txID;
        }

        @Override
        public void run() { this.success = writeQueryPreCommitShard(); }

        private boolean writeQueryPreCommitShard() {
            final int[] queryStatus = {QUERY_RETRY};
            while (queryStatus[0] == QUERY_RETRY) {
                Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getPrimaryStubForShard(shardNum);
                if (stubOpt.isEmpty()) {
                    logger.error("Could not find DataStore for shard {}", shardNum);
                    assert (false);  // TODO:  Retry
                    return false;
                }
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(stubOpt.get().getChannel());
                final CountDownLatch finishLatch = new CountDownLatch(1);
                StreamObserver<WriteQueryPreCommitMessage> observer =
                        stub.writeQueryPreCommit(new StreamObserver<>() {
                            @Override
                            public void onNext(WriteQueryPreCommitResponse writeQueryPreCommitResponse) {
                                queryStatus[0] = writeQueryPreCommitResponse.getReturnCode();
                            }

                            @Override
                            public void onError(Throwable th) {
                                assert (false);
                            }

                            @Override
                            public void onCompleted() {
                                finishLatch.countDown();
                            }
                        });
                final int STEPSIZE = 10000;
                for (int i = 0; i < rowArray.length; i += STEPSIZE) {
                    ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                    R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    WriteQueryPreCommitMessage rowMessage = WriteQueryPreCommitMessage.newBuilder().setShard(shardNum).
                            setSerializedQuery(serializedQuery).setRowData(rowData).setTxID(txID).build();
                    observer.onNext(rowMessage);
                }
                observer.onCompleted();
                try {
                    finishLatch.await();
                } catch (InterruptedException e) {
                    logger.error("Write PreCommit Interrupted: {}", e.getMessage());
                    assert (false);
                }
                if (queryStatus[0] == QUERY_RETRY) {
                    try {
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                    } catch (Throwable ignored) {}
                }
            }
            return queryStatus[0] == 0;
        }

        public boolean isSuccess() {
            return success;
        }
    }

    private class WriteQueryCommitShardThread extends Thread {
        private final boolean commitOrAbort;  // Commit on true, abort on false.
        private final int shardNum;
        private final long txID;

        WriteQueryCommitShardThread(int shardNum, boolean commitOrAbort, long txID) {
            this.shardNum = shardNum;
            this.commitOrAbort = commitOrAbort;
            this.txID = txID;
        }

        @Override
        public void run() { writeQueryCommitShard(); }

        private void writeQueryCommitShard() {
            Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getPrimaryStubForShard(shardNum);
            if (stubOpt.isEmpty()) {
                logger.error("Could not find DataStore for shard {}", shardNum);
                assert(false);  // TODO:  Retry
                return;
            }
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = stubOpt.get();
            WriteQueryCommitMessage rowMessage = WriteQueryCommitMessage.newBuilder().
                    setShard(shardNum).setCommitOrAbort(commitOrAbort).setTxID(txID).build();
            try {
                WriteQueryCommitResponse alwaysEmpty = stub.writeQueryCommit(rowMessage);
            } catch (StatusRuntimeException e) {
                logger.warn("RPC failed: {}", e.getStatus());
                assert(false); // TODO:  Retry
            }
        }
    }

    private class ExecuteReadQueryStageThread<S extends Shard, T extends Serializable, V> extends Thread {
        private final ReadQueryPlan<S, T, V> readQueryPlan;
        private V queryResult;

        ExecuteReadQueryStageThread(ReadQueryPlan<S, T, V> readQueryPlan) {
            this.readQueryPlan = readQueryPlan;
        }

        @Override
        public void run() { this.queryResult = executeReadQueryStage(readQueryPlan); }

        public V executeReadQueryStage(ReadQueryPlan<S, T, V> readQueryPlan) {
            List<Integer> partitionKeys = readQueryPlan.keysForQuery();
            List<Integer> shardNums;
            if (partitionKeys.contains(-1)) {
                // -1 is a wildcard--run on all shards.
                shardNums = IntStream.range(0, numShards).boxed().collect(Collectors.toList());
            } else {
                shardNums = partitionKeys.stream().map(Broker::keyToShard).distinct().collect(Collectors.toList());
            }
            List<ReadQueryShardThread<T>> readQueryShardThreads = new ArrayList<>();
            for (int shardNum : shardNums) {
                ReadQueryShardThread<T> readQueryShardThread = new ReadQueryShardThread<>(shardNum, readQueryPlan);
                readQueryShardThreads.add(readQueryShardThread);
                readQueryShardThread.start();
            }
            List<T> intermediates = new ArrayList<>();
            for (ReadQueryShardThread<T> readQueryShardThread : readQueryShardThreads) {
                try {
                    readQueryShardThread.join();
                } catch (InterruptedException e) {
                    logger.error("Query interrupted: {}", e.getMessage());
                    assert(false);
                }
                Optional<T> intermediate = readQueryShardThread.getIntermediate();
                if (intermediate.isPresent()) {
                    intermediates.add(intermediate.get());
                } else {
                    // TODO:  Query fault tolerance.
                    logger.warn("Query Failure");
                    assert(false);
                }
            }
            return readQueryPlan.aggregateShardQueries(intermediates);
        }

        V getQueryResult() { return this.queryResult; }
        ReadQueryPlan<S, T, V> getReadQueryPlan() { return this.readQueryPlan; }
    }

    private class ReadQueryShardThread<T extends Serializable> extends Thread {
        private final int shardNum;
        private final ReadQueryPlan readQueryPlan;
        private Optional<T> intermediate;

        ReadQueryShardThread(int shardNum, ReadQueryPlan readQueryPlan) {
            this.shardNum = shardNum;
            this.readQueryPlan = readQueryPlan;
        }

        @Override
        public void run() {
            this.intermediate = queryShard(this.shardNum);
        }

        private Optional<T> queryShard(int shard) {
            int queryStatus = QUERY_RETRY;
            ReadQueryResponse readQueryResponse = null;
            while (queryStatus == QUERY_RETRY) {
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
                    readQueryResponse = stub.readQuery(readQuery);
                    queryStatus = readQueryResponse.getReturnCode();
                    assert queryStatus != QUERY_FAILURE;
                } catch (StatusRuntimeException e) {
                    logger.warn("RPC failed: {}", e.getStatus());
                    assert (false); // TODO:  Retry?
                }
                if (queryStatus == QUERY_RETRY) {
                    try {
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                    } catch (Throwable ignored) {}
                }
            }
            ByteString responseByteString = readQueryResponse.getResponse();
            T obj;
            obj = (T) Utilities.byteStringToObject(responseByteString);
            return Optional.of(obj);
        }

        Optional<T> getIntermediate() {
            return this.intermediate;
        }
    }
}


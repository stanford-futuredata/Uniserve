package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Broker {

    private final QueryEngine queryEngine;
    private final BrokerCurator zkCurator;

    private static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    // Map from host/port pairs (used to uniquely identify a DataStore) to stubs.
    private final Map<Pair<String, Integer>, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> connStringToStubMap = new ConcurrentHashMap<>();
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

    public Integer insertRow(List<Row> rows) {
        Map<Integer, List<Row>> shardRowListMap = new HashMap<>();
        for (Row row: rows) {
            int partitionKey = row.getPartitionKey();
            assert(partitionKey >= 0);
            int shard = keyToShard(partitionKey);
            shardRowListMap.computeIfAbsent(shard, (k -> new ArrayList<>())).add(row);
        }
        Map<Integer, Row[]> shardRowArrayMap = shardRowListMap.entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray(new Row[0])));
        for (Integer shard: shardRowArrayMap.keySet()) {
            Row[] rowArray = shardRowArrayMap.get(shard);
            Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getPrimaryStubForShard(shard);
            if (stubOpt.isEmpty()) {
                logger.error("Could not find DataStore for shard {}", shard);  //TODO:  Retry
                return 1;
            }
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = stubOpt.get();
            ByteString rowData;
            try {
                rowData = Utilities.objectToByteString(rowArray);
            } catch (IOException e) {
                logger.error("Row Serialization Failed: {}", e.getMessage());
                assert (false);
                return 1;
            }
            InsertRowMessage rowMessage = InsertRowMessage.newBuilder().setShard(shard).setRowData(rowData).build();
            InsertRowResponse addRowAck;
            try {
                addRowAck = stub.insertRow(rowMessage);
            } catch (StatusRuntimeException e) {
                logger.warn("RPC failed: {}", e.getStatus());
                return 1;
            }
            if (addRowAck.getReturnCode() != 0) {
                return 1;
            }
        }
        return 0;
    }

    public <S extends Shard, T extends Serializable, V> V scheduleQuery(ReadQueryPlan<S, T, V> readQueryPlan) {
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
        List<ExecuteQueryPlanThread> executeQueryPlanThreads = new ArrayList<>();
        while (!unexecutedReadQueryPlans.isEmpty()) {
            // If a subquery thread has finished, store its result and remove the thread.
            List<ExecuteQueryPlanThread> updatedExecuteQueryPlanThreads = new ArrayList<>();
            for (ExecuteQueryPlanThread t : executeQueryPlanThreads) {
                if (!t.isAlive()) {
                    executedQueryPlans.put(t.getReadQueryPlan(), t.getQueryResult());
                } else {
                    updatedExecuteQueryPlanThreads.add(t);
                }
            }
            // If all dependencies of a subquery are resolved, execute the subquery.
            executeQueryPlanThreads = updatedExecuteQueryPlanThreads;
            Set<ReadQueryPlan> updatedUnexecutedReadQueryPlans = new HashSet<>();
            for (ReadQueryPlan q: unexecutedReadQueryPlans) {
                if (executedQueryPlans.keySet().containsAll(q.getSubQueries())) {
                    List<Object> subQueryResults = new ArrayList<>();
                    List<ReadQueryPlan> subQueries = q.getSubQueries();
                    for (ReadQueryPlan sq: subQueries) {
                        subQueryResults.add(executedQueryPlans.get(sq));
                    }
                    q.setSubQueryResults(subQueryResults);
                    ExecuteQueryPlanThread t = new ExecuteQueryPlanThread(q);
                    t.start();
                    executeQueryPlanThreads.add(t);
                } else {
                    updatedUnexecutedReadQueryPlans.add(q);
                }
            }
            unexecutedReadQueryPlans = updatedUnexecutedReadQueryPlans;
        }
        // When all dependencies are resolved, the only query still executing will be the top-level one.  Return its result.
        assert(executeQueryPlanThreads.size() == 1);
        ExecuteQueryPlanThread<S, T, V> finalThread = (ExecuteQueryPlanThread<S, T, V>) executeQueryPlanThreads.get(0);
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

    private BrokerDataStoreGrpc.BrokerDataStoreBlockingStub getStubFromHostPort(Pair<String, Integer> hostPort) {
        BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = connStringToStubMap.getOrDefault(hostPort, null);
        if (stub == null) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(hostPort.getValue0(), hostPort.getValue1()).usePlaintext().build();
            connStringToStubMap.putIfAbsent(hostPort, BrokerDataStoreGrpc.newBlockingStub(channel));
            stub = connStringToStubMap.get(hostPort);
        }
        return stub;
    }

    private Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> getPrimaryStubForShard(int shard) {
        // First, check the local shard-to-server map.
        BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = shardToPrimaryStubMap.getOrDefault(shard, null);
        if (stub == null) {
            // TODO:  This is thread-safe, but might make many redundant requests.
            Pair<String, Integer> hostPort;
            // Then, try to pull it from ZooKeeper.
            Optional<Pair<String, Integer>> hostPortOpt = zkCurator.getShardPrimaryConnectString(shard);
            if (hostPortOpt.isPresent()) {
                hostPort = hostPortOpt.get();
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
                hostPort = Utilities.parseConnectString(r.getConnectString());
            }
            stub = getStubFromHostPort(hostPort);
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
                    Optional<Pair<String, Integer>> primaryHostPortOpt =
                            zkCurator.getShardPrimaryConnectString(shardNum);
                    Optional<List<Pair<String, Integer>>> replicaHostPortsOpt =
                            zkCurator.getShardReplicaConnectStrings(shardNum);
                    if (primaryHostPortOpt.isEmpty() || replicaHostPortsOpt.isEmpty()) {
                        logger.error("ZK has lost information on Shard {}", shardNum);
                        continue;
                    }
                    Pair<String, Integer> primaryHostPort = primaryHostPortOpt.get();
                    BrokerDataStoreGrpc.BrokerDataStoreBlockingStub primaryStub = getStubFromHostPort(primaryHostPort);
                    List<Pair<String, Integer>> replicaHostPorts = replicaHostPortsOpt.get();
                    List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> replicaStubs = new ArrayList<>();
                    for (Pair<String, Integer> replicaHostPort: replicaHostPorts) {
                        replicaStubs.add(getStubFromHostPort(replicaHostPort));
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

    private class ExecuteQueryPlanThread <S extends Shard, T extends Serializable, V> extends Thread {

        private final ReadQueryPlan<S, T, V> readQueryPlan;
        private V queryResult;

        ExecuteQueryPlanThread(ReadQueryPlan<S, T, V> readQueryPlan) {
            this.readQueryPlan = readQueryPlan;
        }

        @Override
        public void run() { this.queryResult = executeQueryPlan(readQueryPlan); }

        public V executeQueryPlan(ReadQueryPlan<S, T, V> readQueryPlan) {
            List<Integer> partitionKeys = readQueryPlan.keysForQuery();
            List<Integer> shards;
            if (partitionKeys.contains(-1)) {
                // -1 is a wildcard--run on all shards.
                shards = IntStream.range(0, numShards).boxed().collect(Collectors.toList());
            } else {
                shards = partitionKeys.stream().map(Broker::keyToShard).distinct().collect(Collectors.toList());
            }
            List<QueryShardThread<T>> queryShardThreads = new ArrayList<>();
            for (int shard : shards) {
                QueryShardThread<T> queryShardThread = new QueryShardThread<T>(shard, readQueryPlan);
                queryShardThreads.add(queryShardThread);
                queryShardThread.start();
            }
            List<T> intermediates = new ArrayList<>();
            for (QueryShardThread<T> queryShardThread : queryShardThreads) {
                try {
                    queryShardThread.join();
                } catch (InterruptedException e) {
                    logger.error("Query interrupted: {}", e.getMessage());
                    assert(false);
                }
                Optional<T> intermediate = queryShardThread.getIntermediate();
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

    private class QueryShardThread<T extends Serializable> extends Thread {
        private final int shard;
        private final ReadQueryPlan readQueryPlan;
        private Optional<T> intermediate;

        QueryShardThread(int shard, ReadQueryPlan readQueryPlan) {
            this.shard = shard;
            this.readQueryPlan = readQueryPlan;
        }

        @Override
        public void run() {
            this.intermediate = queryShard(this.shard);
        }

        private Optional<T> queryShard(int shard) {
            Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getAnyStubForShard(shard);
            if (stubOpt.isEmpty()) {
                logger.warn("Could not find DataStore for shard {}", shard);
                return Optional.empty();
            }
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = stubOpt.get();
            ByteString serializedQuery;
            try {
                serializedQuery = Utilities.objectToByteString(readQueryPlan);
            } catch (IOException e) {
                logger.warn("Query Serialization Failed: {}", e.getMessage());
                return Optional.empty();
            }
            ReadQueryMessage readQuery = ReadQueryMessage.newBuilder().setShard(shard).setSerializedQuery(serializedQuery).build();
            ReadQueryResponse readQueryResponse;
            try {
                readQueryResponse = stub.readQuery(readQuery);
                assert readQueryResponse.getReturnCode() == 0;
            } catch (StatusRuntimeException e) {
                logger.warn("RPC failed: {}", e.getStatus());
                return Optional.empty();
            }
            ByteString responseByteString = readQueryResponse.getResponse();
            T obj;
            try {
                obj = (T) Utilities.byteStringToObject(responseByteString);
            } catch (IOException | ClassNotFoundException e) {
                logger.error("Deserialization failed: {}", e.getMessage());
                return Optional.empty();
            }
            return Optional.of(obj);
        }

        Optional<T> getIntermediate() {
            return this.intermediate;
        }
    }
}


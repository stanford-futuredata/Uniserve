package edu.stanford.futuredata.uniserve.simulator;

import edu.stanford.futuredata.uniserve.coordinator.LoadBalancer;
import ilog.concert.IloException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulate the behavior of a cluster of N-core servers.
 */
public class Simulator {
    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);

    private static final LoadBalancer lb = new LoadBalancer();

    private Integer globalClock = 0;
    private final Integer simulatedMaxMemory = 20;
    private Map<ShardSet, List<Integer>> latencies = new HashMap<>();
    private Map<Integer, List<Integer>> serverLatencies = new HashMap<>();
    private final Integer simulatedNumCores = 4;

    private final int ticksPerSubquery = 10;

    final int numShards;
    final int numServers;

    final int[] shardLoads;
    final int[] memoryMap;

    public Simulator(int numShards, int numServers) {
        this.numShards = numShards;
        this.numServers = numServers;
        shardLoads = new int[numShards];
        memoryMap = new int[numShards];
    }

    public void run(int numIterations) {
        globalClock = 0;
        List<Server> servers = new ArrayList<>();
        List<double[]> shardAssignments = new ArrayList<>();
        for(int serverNum = 0; serverNum < numServers; serverNum++) {
            shardAssignments.add(new double[numShards]);
            servers.add(new Server(serverNum));
        }
        for(int shardNum = 0; shardNum < numShards; shardNum++) {
            int startingServer = shardNum % numServers;
            shardAssignments.get(startingServer)[shardNum] = 1.0;
            memoryMap[shardNum] = 1; // Each server can hold maxMemory shards.
        }
        for(int iterNum = 0; iterNum < numIterations; iterNum++) {
            // Do work on servers.
            for(Server server: servers) {
                server.doWork();
            }
            if (iterNum % ticksPerSubquery == 0) {
                List<Query> queries = generateQueries();
                // Add queries to servers.
                for (Query query : queries) {
                    List<Integer> shardNums = query.getShards();
                    for (Integer shardNum : shardNums) {
                        double randomAssignment = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
                        double currentState = 0;
                        for (int serverNum = 0; serverNum < numServers; serverNum++) {
                            double[] ratios = shardAssignments.get(serverNum);
                            currentState += ratios[shardNum];
                            if (currentState > randomAssignment) {
                                servers.get(serverNum).addQuery(query, shardNum);
                                break;
                            }
                        }
                    }
                }
            }
            // Load-balance.
            if (iterNum > 0 && iterNum % 1000000 == 0 || iterNum == 1000) {
                int[][] currentLocations = new int[numServers][numShards];
                for(int serverNum = 0; serverNum < numServers; serverNum++) {
                    for(int shardNum = 0; shardNum < numShards; shardNum++) {
                        currentLocations[serverNum][shardNum] = shardAssignments.get(serverNum)[shardNum] > 0 ? 1 : 0;
                    }
                }
                Map<Set<Integer>, Integer> sampleQueries = new HashMap<>();
                for (int i = 0; i < 10000; i++) {
                    List<Query> queries = generateQueries();
                    for (Query q: queries) {
                        sampleQueries.merge(new HashSet<>(q.shards), 1, Integer::sum);
                    }
                }
                try {
                    shardAssignments = lb.balanceLoad(numShards, numServers, shardLoads, memoryMap, currentLocations, sampleQueries, simulatedMaxMemory);
                } catch (IloException e) {
                    e.printStackTrace();
                    assert(false);
                }
                for (int shardNum = 0; shardNum < numShards; shardNum++) {
                    shardLoads[shardNum] = 0;
                }
                for (int serverNum = 0; serverNum < numServers; serverNum++) {
                    List<Integer> sLatencies = serverLatencies.get(serverNum);
                    if (!Objects.isNull(sLatencies)) {
                        sLatencies.sort(Integer::compareTo);
                        double p50 = sLatencies.get(sLatencies.size() / 2);
                        double p99 = sLatencies.get((sLatencies.size() * 99) / 100);
                        logger.info("Server {}: {}  Queue size: {}  p50: {} p99: {}",
                                serverNum, shardAssignments.get(serverNum), servers.get(serverNum).queueSizes.stream().mapToInt(i -> i).average().getAsDouble(), p50, p99);
                        servers.get(serverNum).queueSizes.clear();
                        sLatencies.clear();
                    }
                }
                for (Map.Entry<ShardSet, List<Integer>> entry : latencies.entrySet()) {
                    List<Integer> shardSetLatencies = entry.getValue();
                    if (shardSetLatencies.size() > 0) {
                        shardSetLatencies.sort(Integer::compareTo);
                        double p50 = shardSetLatencies.get(shardSetLatencies.size() / 2);
                        double p99 = shardSetLatencies.get((shardSetLatencies.size() * 99) / 100);
                        logger.info("Shardset: {} NumQueries: {} p50: {}, p99: {}", entry.getKey(), shardSetLatencies.size(), p50, p99);
                    }
                }
                latencies.clear();
            }
            globalClock++;
        }

    }

    // Generate queries which load the cluster to 80% of capacity.
    private List<Query> generateQueries() {

        int numRecords = 1000;
        assert(numRecords % numShards == 0);
        int divisor = numRecords / numShards;
        int maxQueryRecords = 200;

        List<Query> queries = new ArrayList<>();
        final int workToGenerate =  (simulatedNumCores * numServers * 80) / 100;
        int workGenerated = 0;
        while(workGenerated < workToGenerate) {
            List<Integer> shardList = new ArrayList<>();
            int startRecord = ThreadLocalRandom.current().nextInt(numRecords);
            int endRecord = startRecord + ThreadLocalRandom.current().nextInt(maxQueryRecords);
            endRecord = Math.min(endRecord, numRecords - 1);
            int startShard = startRecord / divisor;
            int endShard = endRecord / divisor;
            assert(startShard >= 0 && endShard < numShards);
            for(int shardNum = startShard; shardNum <= endShard; shardNum++) {
                shardList.add(shardNum);
            }
            workGenerated += shardList.size();
            queries.add(new Query(shardList, ticksPerSubquery));
        }
        return queries;
    }

    // Each server will divide its cores evenly between all outstanding subqueries, with at most one core per query.
    private class Server {
        private List<Pair<Query, Integer>> queries = new ArrayList<>();
        private List<Integer> queueSizes = new ArrayList<>();

        private final int id;

        public Server(int id) {
            this.id = id;
        }

        public void addQuery(Query query, Integer shardNum) {
            queries.add(new Pair<>(query, shardNum));
        }

        public void doWork() {
            List<Pair<Query, Integer>> finishedQueries = new ArrayList<>();
            double workAvailable = (double) simulatedNumCores;
            double workDonePerShard = 0;
            double epsilon = 0.000001; // for floating-point problems
            while (queries.size() > 0 && workAvailable > 0 + epsilon && workDonePerShard < 1.0 - epsilon) {
                double workPerShard = Math.min(workAvailable / queries.size(), 1.0 - workDonePerShard);
                workDonePerShard += workPerShard;
                workAvailable -= workPerShard * queries.size();
                for (Pair<Query, Integer> queryShardPair : queries) {
                    Query query = queryShardPair.getValue0();
                    int shard = queryShardPair.getValue1();
                    double remainingWork = query.doShardWork(shard, id, workPerShard);
                    if (remainingWork <= 0) { // If less than a full workPerShard amount was needed.
                        finishedQueries.add(queryShardPair);
                        workAvailable -= remainingWork;
                    }
                }
                for (Pair<Query, Integer> finishedQuery : finishedQueries) {
                    queries.remove(finishedQuery);
                }
            }
            queueSizes.add(queries.size());
        }
    }

    // Each query has several subqueries which run on different shards.
    private class Query {
        // Shard num to work remaining.
        private final Map<Integer, Double> subQueries = new HashMap<>();
        // Tick when query starts.
        private final int startTick;
        // Number of outstanding subqueries.
        private int remainingSubqueries;
        private final List<Integer> shards;
        private final Set<Integer> servers = new HashSet<>();

        public Query(List<Integer> shardNums, Integer queryTicks) {
            for(Integer shardNum: shardNums) {
                subQueries.put(shardNum, (double) queryTicks);
                shardLoads[shardNum] += 1;
            }
            startTick = globalClock;
            remainingSubqueries = shardNums.size();
            shards = shardNums;
        }

        public List<Integer> getShards() {
            return shards;
        }

        public double doShardWork(int shardNum, int serverID, double work) {
            assert(work > 0.0 && work <= 1.0);
            servers.add(serverID);
            double workRemaining = subQueries.get(shardNum);
            assert(workRemaining > 0);
            subQueries.put(shardNum, workRemaining - work);
            // Record query latency if query is finished.
            if (workRemaining - work <= 0) {
                remainingSubqueries -= 1;
                if (remainingSubqueries == 0) {
                    ShardSet s = new ShardSet(shards);
                    latencies.putIfAbsent(s, new ArrayList<>());
                    latencies.get(s).add(globalClock - startTick);
                    for(Integer id: servers) {
                        serverLatencies.putIfAbsent(id, new ArrayList<>());
                        serverLatencies.get(id).add(globalClock - startTick);
                    }
                }
            }
            return workRemaining - work;
        }

        @Override
        public String toString() {
            return "Query{" +
                    "subQueries=" + subQueries +
                    '}';
        }
    }

    private static class ShardSet {
        public final List<Integer> shardList;
        public ShardSet(List<Integer> shardList) {
            this.shardList = shardList;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardSet shardSet = (ShardSet) o;
            return shardList.size() == shardSet.shardList.size();
        }

        @Override
        public int hashCode() {
            return shardList.size();
        }

        @Override
        public String toString() {
            return Integer.toString(shardList.size());
        }
    }
}

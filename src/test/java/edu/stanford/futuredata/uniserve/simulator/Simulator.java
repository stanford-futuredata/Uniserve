package edu.stanford.futuredata.uniserve.simulator;

import edu.stanford.futuredata.uniserve.coordinator.LoadBalancer;
import ilog.concert.IloException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulate the behavior of a cluster of completely serial single-core servers.  Report p50 and p99 latencies.
 */
public class Simulator {
    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);

    private Integer globalClock = 0;
    private final Integer maxMemory = 4;
    private Map<ShardSet, List<Integer>> latencies = new HashMap<>();
    private Map<Integer, List<Integer>> serverLatencies = new HashMap<>();

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
        double[][] shardAffinities = new double[numShards][numShards];
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
            List<Query> queries = generateQueries();
            // Add queries to servers.
            for (Query query: queries) {
                List<Integer> shardNums = query.getShards();
                for(Integer shardNum: shardNums) {
                    double randomAssignment = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
                    double currentState = 0;
                    for(int serverNum = 0; serverNum < numServers; serverNum++) {
                        double[] ratios = shardAssignments.get(serverNum);
                        currentState += ratios[shardNum];
                        if (currentState > randomAssignment) {
                            servers.get(serverNum).addQuery(query, shardNum);
                            break;
                        }
                    }
                }
            }
            // Load-balance.
            if (iterNum > 0 && iterNum % 10000 == 0) {
                int[][] currentLocations = new int[numServers][numShards];
                for(int serverNum = 0; serverNum < numServers; serverNum++) {
                    for(int shardNum = 0; shardNum < numShards; shardNum++) {
                        currentLocations[serverNum][shardNum] = shardAssignments.get(serverNum)[shardNum] > 0 ? 1 : 0;
                    }
                }
                try {
                    shardAssignments = LoadBalancer.balanceLoad(numShards, numServers, shardLoads, memoryMap, currentLocations, shardAffinities, maxMemory);
                } catch (IloException e) {
                    e.printStackTrace();
                    assert(false);
                }
                for(int serverNum = 0; serverNum < numServers; serverNum++) {
                    List<Integer> sLatencies = serverLatencies.get(serverNum);
                    sLatencies.sort(Integer::compareTo);
                    double p50 = sLatencies.get(sLatencies.size() / 2);
                    double p99 = sLatencies.get((sLatencies.size() * 99) / 100);
                    logger.info("Server {} Assignment: {}  Average queue size: {}  p50: {} p99: {}", serverNum, shardAssignments.get(serverNum), servers.get(serverNum).queueSizes.stream().mapToInt(i -> i).average().getAsDouble(), p50, p99);
                    servers.get(serverNum).queueSizes.clear();
                    sLatencies.clear();
                }
                for(Map.Entry<ShardSet, List<Integer>> entry: latencies.entrySet()) {
                    List<Integer> shardSetLatencies = entry.getValue();
                    assert (shardSetLatencies.size() > 0);
                    shardSetLatencies.sort(Integer::compareTo);
                    double p50 = shardSetLatencies.get(shardSetLatencies.size() / 2);
                    double p99 = shardSetLatencies.get((shardSetLatencies.size() * 99) / 100);
                    logger.info("Shardset: {} NumQueries: {} p50: {}, p99: {}", entry.getKey(), shardSetLatencies.size(), p50, p99);

                }
                for (int shardNum = 0; shardNum < numShards; shardNum++) {
                    shardLoads[shardNum] = 0;
                }
                latencies.clear();
            }
            globalClock++;
        }

    }

    private List<Query> generateQueries() {
        List<Query> queries = new ArrayList<>();
        final int workToGenerate = (numServers * 8) / 10;
        int workGenerated = 0;
        while(workGenerated < workToGenerate) {
            List<Integer> shardList;
            int querySelector = ThreadLocalRandom.current().nextInt(numShards);
            if (querySelector <= 5) {
                shardList = Arrays.asList(0, 1);
                workGenerated += 2;
            } else {
                shardList = Collections.singletonList(ThreadLocalRandom.current().nextInt(numShards));
                workGenerated += 1;
            }
            queries.add(new Query(shardList, 1));
        }
        return queries;
    }

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
            if (queries.size() > 0) {
                Query firstQuery = queries.get(0).getValue0();
                int firstQueryShard = queries.get(0).getValue1();
                firstQuery.doShardWork(firstQueryShard, id);
                if (!firstQuery.checkShardWork(firstQueryShard)) {
                    queries.remove(0);
                }
            }
            queueSizes.add(queries.size());
        }
    }

    private class Query {
        // Shard num to work remaining.
        private final Map<Integer, Integer> subQueries = new HashMap<>();
        // Tick when query starts.
        private final int startTick;
        // Number of outstanding subqueries.
        private int remainingSubqueries;
        private final List<Integer> shards;
        private final Set<Integer> servers = new HashSet<>();

        public Query(List<Integer> shardNums, Integer queryTicks) {
            for(Integer shardNum: shardNums) {
                subQueries.put(shardNum, queryTicks);
            }
            startTick = globalClock;
            remainingSubqueries = shardNums.size();
            shards = shardNums;
        }

        public List<Integer> getShards() {
            return shards;
        }

        public boolean checkShardWork(int shardNum) {
            return subQueries.get(shardNum) > 0;
        }

        public void doShardWork(int shardNum, int serverID) {
            servers.add(serverID);
            int workRemaining = subQueries.get(shardNum);
            assert(workRemaining > 0);
            subQueries.put(shardNum, workRemaining - 1);
            // Record query latency if query is finished.
            if (workRemaining == 1) {
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
                shardLoads[shardNum]++;
            }
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
            if (shardList.size() == 1 && shardSet.shardList.size() == 1) {
                return true;
            }
            return shardList.equals(shardSet.shardList);
        }

        @Override
        public int hashCode() {
            if (shardList.size() == 1) {
                return 1;
            } else {
                return shardList.stream().mapToInt(Object::hashCode).sum();
            }
        }

        @Override
        public String toString() {
            if (shardList.size() == 1) {
                return "[1]";
            } else {
                return shardList.toString();
            }
        }
    }
}

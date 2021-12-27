package edu.stanford.futuredata.uniserve.coordinator;

import ilog.concert.*;
import ilog.cplex.IloCplex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ParallelismLoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(ParallelismLoadBalancer.class);
    public static boolean verbose = true;

    final static double mipGap = 0.05;
    final int maxQuerySamples = 500;

    /**
     * Generate an assignment of shards to servers.
     * @param numShards  Number of shards.
     * @param numServers  Number of servers.
     * @param shardLoads Amount of load on each shard.
     * @param shardMemoryUsages Memory usage of each shard.
     * @param currentLocations Entry [x][y] is 1 if server x has a copy of shard y and 0 otherwise.
     * @param sampleQueries A map from sets of shards to the number of queries that touched precisely that set.
     * @param maxMemory  Maximum server memory.
     * @return Entry [x][y] is the percentage of queries for shard y that should be routed to server x.
     * @throws IloException
     */

    public List<double[]> balanceLoad(Integer numShards, Integer numServers,
                                      int[] shardLoads, int[] shardMemoryUsages, int[][] currentLocations,
                                      Map<Set<Integer>, Integer> sampleQueries,
                                      Integer maxMemory) throws IloException {
        assert(shardLoads.length == numShards);
        assert(shardMemoryUsages.length == numShards);
        assert(currentLocations.length == numServers);
        for (int[] currentLocation : currentLocations) {
            assert (currentLocation.length == numShards);
        }

        int[][] transferCosts = currentLocations.clone();
        for(int i = 0; i < transferCosts.length; i++) {
            transferCosts[i] = Arrays.stream(transferCosts[i]).map(j -> j == 0 ? 1 : 0).toArray();
        }

        // Begin parallel objective.
        IloCplex cplex = new IloCplex();
        if (!verbose) {
            cplex.setOut(null);
        }

        List<IloNumVar[]> r = new ArrayList<>();
        List<IloNumVar[]> x = new ArrayList<>();
        for (int i = 0; i < numServers; i++) {
            r.add(cplex.numVarArray(numShards, 0, 1));
            x.add(cplex.intVarArray(numShards, 0, 1));
        }

        List<Set<Integer>> sampleQueryKeys = new ArrayList<>(sampleQueries.keySet());
        sampleQueryKeys = sampleQueryKeys.stream().filter(k -> k.size() > 1).collect(Collectors.toList());
        sampleQueryKeys = sampleQueryKeys.stream().sorted(Comparator.comparing(sampleQueries::get).reversed()).limit(maxQuerySamples).collect(Collectors.toList());

        // Minimize sum of query worst-case times, weighted by query frequency.
        int numSampleQueries = sampleQueryKeys.size();
        IloIntVar[] m = cplex.intVarArray(numSampleQueries, 0, 20); // Each entry is the maximum number of that query's shards on the same server.

        for(int serverNum = 0; serverNum < numServers; serverNum++) {
            int q = 0;
            for (Set<Integer> shards : sampleQueryKeys) {
                IloNumExpr e = cplex.constant(0);
                for (Integer shardNum : shards) {
                    e = cplex.sum(e, x.get(serverNum)[shardNum]);
                }
                cplex.addLe(e, m[q]);
                q++;
            }
        }

        int[] queryWeights = sampleQueryKeys.stream().map(sampleQueries::get).mapToInt(i -> i).toArray();
        IloObjective parallelObjective = cplex.minimize(cplex.scalProd(m, queryWeights));
        cplex.add(parallelObjective);

        setCoreConstraints(cplex, r, x, numShards, numServers, shardLoads, shardMemoryUsages, maxMemory);

        // Solve parallel objective.
        double[] optimalM = null;
        if (numSampleQueries > 0) {
            cplex.solve();
            optimalM = cplex.getValues(m);
        }

        // Begin transfer objective.
        cplex = new IloCplex();
        cplex.setParam(IloCplex.Param.MIP.Tolerances.MIPGap, mipGap);
        if (!verbose) {
            cplex.setOut(null);
        }
        r = new ArrayList<>();
        x = new ArrayList<>();
        for (int i = 0; i < numServers; i++) {
            r.add(cplex.numVarArray(numShards, 0, 1));
            x.add(cplex.intVarArray(numShards, 0, 1));
        }

        // Minimize transfer costs.
        IloNumExpr[] transferCostList = new IloNumExpr[numServers];
        for (int i = 0; i < numServers; i++) {
            transferCostList[i] = cplex.scalProd(x.get(i), transferCosts[i]);
        }
        IloObjective transferObjective = cplex.minimize(cplex.sum(transferCostList));
        cplex.add(transferObjective);

        for(int serverNum = 0; serverNum < numServers; serverNum++) { // Ensure transfer solution conforms to parallelism solution.
            int q = 0;
            for (Set<Integer> shards : sampleQueryKeys) {
                IloNumExpr e = cplex.constant(0);
                for (Integer shardNum : shards) {
                    e = cplex.sum(e, x.get(serverNum)[shardNum]);
                }
                cplex.addLe(e, optimalM[q]);
                q++;
            }
        }

        setCoreConstraints(cplex, r, x, numShards, numServers, shardLoads, shardMemoryUsages, maxMemory);

        // Solve transfer objective.
        cplex.solve();

        List<double[]> optimalR = new ArrayList<>();
        for (int i = 0; i < numServers; i++) {
            optimalR.add(cplex.getValues(r.get(i)));
        }
        return optimalR;
    }


    // Set the load, memory, and sanity constraints.
    private static void setCoreConstraints(IloCplex cplex, List<IloNumVar[]> r, List<IloNumVar[]> x, Integer numShards, Integer numServers,
                                           int[] shardLoads, int[] shardMemoryUsages,
                                           Integer maxMemory) throws IloException {
        double averageLoad = (double) Arrays.stream(shardLoads).sum() / numServers;
        double epsilon = averageLoad / 20;

        for (int i = 0; i < numServers; i++) {
            cplex.addLe(cplex.scalProd(shardLoads, r.get(i)), averageLoad + epsilon); // Max load constraint
            cplex.addGe(cplex.scalProd(shardLoads, r.get(i)), averageLoad - epsilon); // Min load constraint
        }

        for (int i = 0; i < numServers; i++) {
            cplex.addLe(cplex.scalProd(shardMemoryUsages, x.get(i)), maxMemory); // Memory constraint
        }
        for (int i = 0; i < numServers; i++) {
            for (int j = 0; j < numShards; j++) {
                cplex.addLe(r.get(i)[j], x.get(i)[j]); // Ensure x_ij is 1 if r_ij is positive.
            }
        }

        for (int j = 0; j < numShards; j++) {
            IloNumVar[] rShardServers = new IloNumVar[numServers];
            for (int i = 0; i < numServers; i++) {
                rShardServers[i] = r.get(i)[j];
            }
            cplex.addEq(cplex.sum(rShardServers), 1); // Require sum of r for each shard to be 1.
        }

        for (int j = 0; j < numShards; j++) {
            IloNumVar[] xShardServers = new IloNumVar[numServers];
            for (int i = 0; i < numServers; i++) {
                xShardServers[i] = x.get(i)[j];
            }
            cplex.addGe(cplex.sum(xShardServers), 1); // Require each shard to appear on at least one server.
        }
    }
}

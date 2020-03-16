package edu.stanford.futuredata.uniserve.coordinator;

import ilog.concert.*;
import ilog.cplex.IloCplex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class LoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);
    public static boolean verbose = true;

    List<double[]> lastR;
    List<double[]> lastX;
    double[] lastM;
    int lastNumServers = 0;
    int lastNumShards = 0;

    final static double mipGap = 0.1;

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

        final int maxQuerySamples = 5 * numShards;
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

        int[] queryWeights = sampleQueryKeys.stream().map(sampleQueries::get).mapToInt(i ->i).toArray();
        IloObjective parallelObjective = cplex.minimize(cplex.scalProd(m, queryWeights));
        cplex.add(parallelObjective);

        setCoreConstraints(cplex, r, x, numShards, numServers, shardLoads, shardMemoryUsages, maxMemory);

        // Warm Start
        if (numSampleQueries > 0 && lastNumServers == numServers && lastNumShards == numShards && !Objects.isNull(lastM) && lastM.length == m.length) {
            for(int i = 0; i < numServers; i++) {
                cplex.addMIPStart(r.get(i), lastR.get(i));
                cplex.addMIPStart(x.get(i), lastX.get(i));
            }
            cplex.addMIPStart(m, lastM);
        }

        // Solve parallel objective.
        cplex.setParam(IloCplex.Param.MIP.Tolerances.MIPGap, mipGap);
        cplex.solve();
        // Result of parallel objective.
        if (numSampleQueries > 0) {
            lastM = cplex.getValues(m);
        } else {
            lastM = new double[m.length];
            Arrays.fill(lastM, 20.0);
        }

        // Begin transfer objective.
        cplex = new IloCplex();
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

        for(int serverNum = 0; serverNum < numServers; serverNum++) {
            int q = 0;
            for (Set<Integer> shards : sampleQueryKeys) {
                IloNumExpr e = cplex.constant(0);
                for (Integer shardNum : shards) {
                    e = cplex.sum(e, x.get(serverNum)[shardNum]);
                }
                cplex.addLe(e, lastM[q]);
                q++;
            }
        }

        setCoreConstraints(cplex, r, x, numShards, numServers, shardLoads, shardMemoryUsages, maxMemory);

        // Warm Start
        if (numShards > 0 && lastNumServers == numServers && lastNumShards == numShards) {
            for(int i = 0; i < numServers; i++) {
                cplex.addMIPStart(r.get(i), lastR.get(i));
                cplex.addMIPStart(x.get(i), lastX.get(i));
            }
        }

        // Solve transfer objective.
        cplex.setParam(IloCplex.Param.MIP.Tolerances.MIPGap, mipGap);
        cplex.solve();

        lastNumShards = numShards;
        lastNumServers = numServers;
        lastR = new ArrayList<>();
        lastX = new ArrayList<>();
        for (int i = 0; i < numServers; i++) {
            lastR.add(cplex.getValues(r.get(i)));
            lastX.add(cplex.getValues(x.get(i)));
        }
        return lastR;
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
    }
}

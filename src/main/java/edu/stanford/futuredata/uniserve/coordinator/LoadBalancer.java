package edu.stanford.futuredata.uniserve.coordinator;

import ilog.concert.IloException;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);
    public static boolean verbose = true;

    public static List<double[]> balanceLoad(Integer numShards, Integer numServers,
                                   int[] shardLoads, int[] shardMemoryUsages, int[][] currentLocations,
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

        IloNumExpr[] transferCostList = new IloNumExpr[numServers];
        for (int i = 0; i < numServers; i++) {
            transferCostList[i] = cplex.scalProd(x.get(i), transferCosts[i]);
        }
        cplex.addMinimize(cplex.sum(transferCostList));


        setConstraints(cplex, r, x, numShards, numServers, shardLoads, shardMemoryUsages, maxMemory);

        assert(cplex.solve());

        List<double[]> returnR = new ArrayList<>();
        for (int i = 0; i < numServers; i++) {
            returnR.add(cplex.getValues(r.get(i)));
        }
        return returnR;
    }


    // Set the constraints of the load-balancing optimization problem on a solver and its variables.
    private static void setConstraints(IloCplex cplex, List<IloNumVar[]> r, List<IloNumVar[]> x, Integer numShards, Integer numServers,
                        int[] shardLoads, int[] shardMemoryUsages,
                        Integer maxMemory) throws IloException {

        double averageLoad = (double) Arrays.stream(shardLoads).sum() / numServers;
        double epsilon = averageLoad / 10;

        for (int i = 0; i < numServers; i++) {
            cplex.addLe(cplex.scalProd(shardLoads, r.get(i)), averageLoad + epsilon); // Max load constraint
            // cplex.addGe(cplex.scalProd(shardLoads, r.get(i)), averageLoad - epsilon); // Min load constraint
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

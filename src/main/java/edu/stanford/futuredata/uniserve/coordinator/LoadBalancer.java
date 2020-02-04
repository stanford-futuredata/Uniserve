package edu.stanford.futuredata.uniserve.coordinator;

import ilog.concert.IloException;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);

    public static void balanceLoad(Integer numShards, Integer numServers,
                                   double[] shardLoads, double[] shardMemoryUsages, int[][] currentLocations,
                                   Integer maxMemory) throws IloException {
        assert(shardLoads.length == numShards);
        assert(shardMemoryUsages.length == numShards);
        assert(currentLocations.length == numServers);
        for (int i = 0; i < currentLocations.length; i++) {
            assert(currentLocations[i].length == numShards);
        }
        int[][] transferCosts = currentLocations.clone();
        for(int i = 0; i < transferCosts.length; i++) {
            transferCosts[i] = Arrays.stream(transferCosts[i]).map(j -> j == 0 ? 1 : 0).toArray();
        }

        IloCplex cplex = new IloCplex();

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

        double averageLoad = Arrays.stream(shardLoads).sum() / numServers;
        double epsilon = averageLoad / 5;

        for (int i = 0; i < numServers; i++) {
            cplex.addLe(cplex.scalProd(shardLoads, r.get(i)), averageLoad + epsilon); // Max load constraint
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
            cplex.addEq(cplex.sum(rShardServers), 1);
        }

        cplex.solve();

        for (int i = 0; i < numServers; i++) {
            logger.info("Server {}: x: {} r: {}", i, cplex.getValues(x.get(i)), cplex.getValues(r.get(i)));
        }


    }
}

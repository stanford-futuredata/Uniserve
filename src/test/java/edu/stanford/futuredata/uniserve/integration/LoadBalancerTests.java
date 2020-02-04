package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.coordinator.LoadBalancer;
import ilog.concert.IloException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadBalancerTests {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerTests.class);

    @Test
    public void testBasicLoadBalancer() throws IloException {
        logger.info("testBasicLoadBalancer");

        int numShards = 4;
        int numServers = 2;
        double[] shardLoads = new double[]{1., 2., 3., 20.};
        double[] memoryUsages = new double[]{9., 1., 1., 1.};
        int[][] currentLocations = new int[][]{new int[]{1, 1, 1, 1}, new int[]{0, 0, 0, 0}};
        int maxMemory = 10;

        LoadBalancer.balanceLoad(numShards, numServers, shardLoads, memoryUsages, currentLocations, maxMemory);

    }

}

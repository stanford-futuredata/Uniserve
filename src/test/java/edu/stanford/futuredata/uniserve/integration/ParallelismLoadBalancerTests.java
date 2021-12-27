package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.coordinator.ParallelismLoadBalancer;
import ilog.concert.IloException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParallelismLoadBalancerTests {
    private static final Logger logger = LoggerFactory.getLogger(ParallelismLoadBalancerTests.class);

    private static String zkHost = "127.0.0.1";
    private static Integer zkPort = 2181;

    @BeforeAll
    static void startUpCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @AfterEach
    private void unitTestCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @Test
    public void testBalanceLoadFunction() throws IloException {
        logger.info("testBalanceLoadFunction");

        int numShards = 4;
        int numServers = 2;
        int[] shardLoads = new int[]{1, 2, 3, 20};
        int[] memoryUsages = new int[]{9, 1, 1, 1};
        int[][] currentLocations = new int[][]{new int[]{1, 1, 1, 1}, new int[]{0, 0, 0, 0}};
        int maxMemory = 10;

        List<double[]> returnR = new ParallelismLoadBalancer().balanceLoad(numShards, numServers, shardLoads, memoryUsages, currentLocations, new HashMap<>(), maxMemory);
        logger.info("{} {}", returnR.get(0), returnR.get(1));
        double averageLoad = IntStream.of(shardLoads).sum() / (double) numServers;
        for(double[] Rs: returnR) {
            double serverLoad = 0;
            for(int i = 0; i < numShards; i++) {
                serverLoad += Rs[i] * shardLoads[i];
            }
            assertTrue(serverLoad <= averageLoad * 1.05);
        }
    }
}

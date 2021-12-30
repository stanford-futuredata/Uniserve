package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.coordinator.ParallelismLoadBalancer;
import ilog.concert.IloException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Test
    public void testBalanceWithParallelism() throws IloException {
        logger.info("testBalanceWithParallelism");

        int numShards = 4;
        int numServers = 2;
        int[] shardLoads = new int[]{1, 2, 3, 20};
        int[] memoryUsages = new int[]{8, 1, 1, 1};
        int[][] currentLocations = new int[][]{new int[]{0, 1, 1, 1}, new int[]{1, 0, 0, 1}};
        int maxMemory = 10;

        Map<Set<Integer>, Integer> sampleQueries = new HashMap<>();
        sampleQueries.put(Set.of(1, 2), 100);
        List<double[]> returnR = new ParallelismLoadBalancer().balanceLoad(numShards, numServers, shardLoads,
                        memoryUsages, currentLocations, sampleQueries, maxMemory);
        logger.info("{} {}", returnR.get(0), returnR.get(1));
        double averageLoad = IntStream.of(shardLoads).sum() / (double) numServers;
        for(double[] Rs: returnR) {
            double serverLoad = 0;
            if (Rs[1] > 0) {
                assertEquals(Rs[2], 0);
            }
            for(int i = 0; i < numShards; i++) {
                serverLoad += Rs[i] * shardLoads[i];
            }
            assertTrue(serverLoad <= averageLoad * 1.06);
        }
    }

    @Test
    public void testBigBalance() throws IloException {
        logger.info("testBigBalance");

        int numShards = 100;
        int numServers = 10;
        int maxMemory = 10;
        int queryLength = 10;

        int[] shardLoads = new int[numShards];
        for (int i = 0; i < numShards; i++) {
            shardLoads[i] = 11;
        }
        int [] memoryUsages = new int[numShards];
        for (int i = 0; i < numShards; i++) {
            memoryUsages[i] = 1;
        }
        int[][] currentLocations = new int[numServers][numShards];
        for (int shardNum = 0; shardNum < numShards; shardNum++) {
            int serverNum = ThreadLocalRandom.current().nextInt(numServers);
            currentLocations[serverNum][shardNum] = 1;
        }
        Map<Set<Integer>, Integer> sampleQueries = new HashMap<>();
        for (int shardNum = 0; shardNum < numShards; shardNum++) {
            Set<Integer> set = new HashSet<>();
            for (int i = 0; i < queryLength; i++) {
                set.add((shardNum + i) % numShards);
            }
            sampleQueries.put(set, 1);
        }

        List<double[]> returnR = new ParallelismLoadBalancer().balanceLoad(numShards, numServers, shardLoads,
                memoryUsages, currentLocations, sampleQueries, maxMemory);
        for (int i = 0; i < numServers; i++) {
            int sum = 0;
            for (int j = 0; j < numShards; j++) {
                double num = returnR.get(i)[j];
                if (num > 0.0001) {
                    // logger.info("Server: {} Shard: {} R: {}", i, j, num);
                    sum += 1;
                }
            }
            assert(sum <= maxMemory);
        }
    }
}

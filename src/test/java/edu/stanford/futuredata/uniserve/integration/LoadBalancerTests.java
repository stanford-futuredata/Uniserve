package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.LoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.*;
import ilog.concert.IloException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoadBalancerTests {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerTests.class);

    private static String zkHost = "127.0.0.1";
    private static Integer zkPort = 2181;

    @BeforeAll
    static void startUpCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @Test
    public void testBalanceLoadFunction() throws IloException {
        logger.info("testBalanceLoadFunction");

        int numShards = 4;
        int numServers = 2;
        double[] shardLoads = new double[]{1., 2., 3., 20.};
        double[] memoryUsages = new double[]{9., 1., 1., 1.};
        int[][] currentLocations = new int[][]{new int[]{1, 1, 1, 1}, new int[]{0, 0, 0, 0}};
        int maxMemory = 10;

        LoadBalancer.balanceLoad(numShards, numServers, shardLoads, memoryUsages, currentLocations, maxMemory);

    }

    @Test
    public void testLoadBalancer() {
        logger.info("testLoadBalancer");
        int numShards = 2;
        Coordinator coordinator = new Coordinator(zkHost, zkPort, "127.0.0.1", 7778);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        List<DataStore<KVRow, KVShard>> dataStores = new ArrayList<>();
        int num_datastores = 4;
        for (int i = 0; i < num_datastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(null, new KVShardFactory(),
                    Path.of("/var/tmp/KVUniserve"), zkHost, zkPort,"127.0.0.1",  8100 + i);
            dataStore.runPingDaemon = false;
            int d_r = dataStore.startServing();
            assertEquals(0, d_r);
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), numShards);
        List<KVRow> rows = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            rows.add(new KVRow(i, i));
        }
        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, rows);
        assertTrue(writeSuccess);

        ReadQueryPlan<KVShard, Integer, Integer> readQueryPlan = new KVReadQueryPlanSumGet(Collections.singletonList(1));
        Integer queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(1), queryResponse);

        readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 4));
        queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(5), queryResponse);

        readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(55), queryResponse);

        Map<Integer, Integer> collectedLoad = coordinator.collectLoadStatistics();
        assertEquals(2, collectedLoad.get(0));
        assertEquals(3, collectedLoad.get(1));

        for (int i = 0; i < num_datastores; i++) {
            dataStores.get(i).shutDown();
        }
        coordinator.stopServing();
        broker.shutdown();
    }

}

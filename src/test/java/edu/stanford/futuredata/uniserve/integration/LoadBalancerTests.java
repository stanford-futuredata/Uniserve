package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.LoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVQueryEngine;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShardFactory;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVReadQueryPlanGet;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVWriteQueryPlanInsert;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.*;

public class LoadBalancerTests {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerTests.class);

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
    public void testLoadBalancer() {
        logger.info("testLoadBalancer");
        ConsistentHash c = new ConsistentHash();
        c.addBucket(0);
        c.addBucket(1);
        c.addBucket(2);
        c.addBucket(3);
        Map<Integer, Integer> shardLoads = Map.of(0, 1, 1, 1, 2, 1, 3, 1);
        LoadBalancer.balanceLoad(shardLoads, c);
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                if (i != j) {
                    assertNotEquals(c.getBucket(i), c.getBucket(j));
                }
            }
        }
    }

    @Test
    public void testCoordinatorLoadBalance() {
        logger.info("testCoordinatorLoadBalance");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, zkHost, zkPort, "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        int numDataStores = 4;
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        for (int i = 0; i < numDataStores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8200 + i, -1);
            dataStore.runPingDaemon = false;
            int d_r = dataStore.startServing();
            assertEquals(0, d_r);
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        assertTrue(broker.createTable("table1", numShards));

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert("table1");
        assertTrue(broker.writeQuery(writeQueryPlan,
                List.of(new KVRow(0, 0), new KVRow(1, 1), new KVRow(2, 2), new KVRow(3, 3))));

        AnchoredReadQueryPlan<KVShard, Integer> zero = new KVReadQueryPlanGet("table1",0);
        assertEquals(Integer.valueOf(0), broker.anchoredReadQuery(zero));
        AnchoredReadQueryPlan<KVShard, Integer> one = new KVReadQueryPlanGet("table1",1);
        assertEquals(Integer.valueOf(1), broker.anchoredReadQuery(one));
        AnchoredReadQueryPlan<KVShard, Integer> two = new KVReadQueryPlanGet("table1", 2);
        assertEquals(Integer.valueOf(2), broker.anchoredReadQuery(two));
        AnchoredReadQueryPlan<KVShard, Integer> three = new KVReadQueryPlanGet("table1", 3);
        assertEquals(Integer.valueOf(3), broker.anchoredReadQuery(three));

        Map<Integer, Integer> load = coordinator.collectLoad().getValue0();
        Pair<Set<Integer>, Set<Integer>> lostGained = LoadBalancer.balanceLoad(load, coordinator.consistentHash);
        coordinator.assignShards(lostGained.getValue0(), lostGained.getValue1());
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                if (i != j) {
                    assertNotEquals(coordinator.consistentHash.getBucket(i),
                            coordinator.consistentHash.getBucket(j));
                }
            }
        }

        assertEquals(Integer.valueOf(0), broker.anchoredReadQuery(zero));
        assertEquals(Integer.valueOf(1), broker.anchoredReadQuery(one));
        assertEquals(Integer.valueOf(2), broker.anchoredReadQuery(two));
        assertEquals(Integer.valueOf(3), broker.anchoredReadQuery(three));

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }
}

package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVQueryEngine;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShardFactory;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVReadQueryPlanGet;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVWriteQueryPlanInsert;
import edu.stanford.futuredata.uniserve.localcloud.LocalCoordinatorCloud;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.*;

public class AutoScalingTests {
    private static final Logger logger = LoggerFactory.getLogger(AutoScalingTests.class);

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
    public void testBasicAutoScaling() throws InterruptedException {
        logger.info("testBasicAutoScaling");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(new LocalCoordinatorCloud<KVRow, KVShard>(new KVShardFactory()),
                zkHost, zkPort, "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        assertTrue(coordinator.startServing());

        coordinator.addDataStore();
        coordinator.addDataStore();
        coordinator.addDataStore();

        coordinator.cachedQPSLoad = Map.of(0, 1, 1, 1, 2, 1, 3, 1,
                Broker.SHARDS_PER_TABLE, 0, Broker.SHARDS_PER_TABLE + 1, 0,
                Broker.SHARDS_PER_TABLE + 2, 0, Broker.SHARDS_PER_TABLE + 3, 0);

        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());

        assertTrue(broker.createTable("table1", numShards));
        assertTrue(broker.createTable("table2", numShards));

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan;
        writeQueryPlan = new KVWriteQueryPlanInsert("table1");
        assertTrue(broker.writeQuery(writeQueryPlan,
                List.of(new KVRow(0, 0), new KVRow(1, 1), new KVRow(2, 2), new KVRow(3, 3))));
        writeQueryPlan = new KVWriteQueryPlanInsert("table2");
        assertTrue(broker.writeQuery(writeQueryPlan,
                List.of(new KVRow(0, 0), new KVRow(1, 1), new KVRow(2, 2), new KVRow(3, 3))));

        coordinator.addDataStore();

        AnchoredReadQueryPlan<KVShard, Integer> zero = new KVReadQueryPlanGet("table1",0);
        assertEquals(Integer.valueOf(0), broker.anchoredReadQuery(zero));
        AnchoredReadQueryPlan<KVShard, Integer> one = new KVReadQueryPlanGet("table1",1);
        assertEquals(Integer.valueOf(1), broker.anchoredReadQuery(one));
        AnchoredReadQueryPlan<KVShard, Integer> two = new KVReadQueryPlanGet("table1", 2);
        assertEquals(Integer.valueOf(2), broker.anchoredReadQuery(two));
        AnchoredReadQueryPlan<KVShard, Integer> three = new KVReadQueryPlanGet("table1", 3);
        assertEquals(Integer.valueOf(3), broker.anchoredReadQuery(three));

        Thread.sleep(1000);

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                if (i != j) {
                    assertNotEquals(coordinator.consistentHash.getBucket(i), coordinator.consistentHash.getBucket(j));
                }
            }
        }

        assertEquals(Integer.valueOf(0), broker.anchoredReadQuery(zero));
        assertEquals(Integer.valueOf(1), broker.anchoredReadQuery(one));
        assertEquals(Integer.valueOf(2), broker.anchoredReadQuery(two));
        assertEquals(Integer.valueOf(3), broker.anchoredReadQuery(three));

        coordinator.removeDataStore();

        assertEquals(Integer.valueOf(0), broker.anchoredReadQuery(zero));
        assertEquals(Integer.valueOf(1), broker.anchoredReadQuery(one));
        assertEquals(Integer.valueOf(2), broker.anchoredReadQuery(two));
        assertEquals(Integer.valueOf(3), broker.anchoredReadQuery(three));

        coordinator.removeDataStore();

        assertEquals(Integer.valueOf(0), broker.anchoredReadQuery(zero));
        assertEquals(Integer.valueOf(1), broker.anchoredReadQuery(one));
        assertEquals(Integer.valueOf(2), broker.anchoredReadQuery(two));
        assertEquals(Integer.valueOf(3), broker.anchoredReadQuery(three));

        coordinator.removeDataStore();

        assertEquals(Integer.valueOf(0), broker.anchoredReadQuery(zero));
        assertEquals(Integer.valueOf(1), broker.anchoredReadQuery(one));
        assertEquals(Integer.valueOf(2), broker.anchoredReadQuery(two));
        assertEquals(Integer.valueOf(3), broker.anchoredReadQuery(three));

        coordinator.stopServing();
        broker.shutdown();
    }
}

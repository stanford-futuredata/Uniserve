package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KVStoreTests {

    private static final Logger logger = LoggerFactory.getLogger(KVStoreTests.class);

    @After
    public void cleanUp() throws Exception {
        // Clean up ZooKeeper
        String connectString = "localhost:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        for (String child: cf.getChildren().forPath("/")) {
            if (!child.equals("zookeeper")) {
                cf.delete().deletingChildrenIfNeeded().forPath("/" + child);
            }
        }
    }

    @Test
    public void testSingleKey() {
        int numShards = 1;
        Coordinator coordinator = new Coordinator("localhost", 2181, 7777);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        DataStore dataStore = new DataStore(8888, new KVShardFactory());
        int d_r = dataStore.startServing();
        assertEquals(0, d_r);
        Broker broker = new Broker("127.0.0.1", 2181, new KVQueryEngine(numShards));

        int addRowReturnCode = broker.insertRow(new KVRow(1, 2));
        assertEquals(0, addRowReturnCode);

        QueryPlan queryPlan = new KVQueryPlanGet(1);
        Pair<Integer, String> queryResponse = broker.readQuery(queryPlan);
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals("2", queryResponse.getValue1());

        dataStore.stopServing();
        coordinator.stopServing();
    }

    @Test
    public void testMultiKey() {
        int numShards = 2;
        Coordinator coordinator = new Coordinator("localhost", 2181, 7777);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        DataStore dataStore = new DataStore(8888, new KVShardFactory());
        int d_r = dataStore.startServing();
        assertEquals(0, d_r);
        Broker broker = new Broker("127.0.0.1", 2181, new KVQueryEngine(numShards));

        int addRowReturnCode = broker.insertRow(new KVRow(1, 2));
        assertEquals(0, addRowReturnCode);
        addRowReturnCode = broker.insertRow(new KVRow(2, 3));
        assertEquals(0, addRowReturnCode);

        QueryPlan queryPlan = new KVQueryPlanSumGet(Collections.singletonList(1));
        Pair<Integer, String> queryResponse = broker.readQuery(queryPlan);
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals("2", queryResponse.getValue1());

        queryPlan = new KVQueryPlanSumGet(Collections.singletonList(2));
        queryResponse = broker.readQuery(queryPlan);
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals("3", queryResponse.getValue1());

        queryPlan = new KVQueryPlanSumGet(Arrays.asList(1, 2));
        queryResponse = broker.readQuery(queryPlan);
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals("5", queryResponse.getValue1());

        dataStore.stopServing();
        coordinator.stopServing();
    }
}

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
import java.util.List;

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
        broker.shutdown();
    }

    @Test
    public void testMultiKey() {
        int numShards = 2;
        Coordinator coordinator = new Coordinator("localhost", 2181, 7777);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        List<DataStore> dataStores = new ArrayList<>();
        int num_datastores = 4;
        for (int i = 0; i < num_datastores; i++) {
            DataStore dataStore = new DataStore(8888, new KVShardFactory());
            int d_r = dataStore.startServing();
            assertEquals(0, d_r);
            dataStores.add(dataStore);
        }
        Broker broker = new Broker("127.0.0.1", 2181, new KVQueryEngine(numShards));

        for (int i = 1; i < 11; i++) {
            int addRowReturnCode = broker.insertRow(new KVRow(i, i));
            assertEquals(0, addRowReturnCode);
        }

        QueryPlan queryPlan = new KVQueryPlanSumGet(Collections.singletonList(1));
        Pair<Integer, String> queryResponse = broker.readQuery(queryPlan);
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals("1", queryResponse.getValue1());

        queryPlan = new KVQueryPlanSumGet(Arrays.asList(1, 5));
        queryResponse = broker.readQuery(queryPlan);
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals("6", queryResponse.getValue1());

        queryPlan = new KVQueryPlanSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        queryResponse = broker.readQuery(queryPlan);
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals("55", queryResponse.getValue1());

        for (int i = 0; i < num_datastores; i++) {
           dataStores.get(i).stopServing();
        }
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testSimultaneousReadQuery() throws InterruptedException {
        int numShards = 2;
        Coordinator coordinator = new Coordinator("localhost", 2181, 7777);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        List<DataStore> dataStores = new ArrayList<>();
        int num_datastores = 4;
        for (int i = 0; i < num_datastores; i++) {
            DataStore dataStore = new DataStore(8888, new KVShardFactory());
            int d_r = dataStore.startServing();
            assertEquals(0, d_r);
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker("127.0.0.1", 2181, new KVQueryEngine(numShards));

        for (int i = 0; i < 100; i++) {
            int addRowReturnCode = broker.insertRow(new KVRow(i, i));
            assertEquals(0, addRowReturnCode);
        }
        List<BrokerThread> brokerThreads = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            BrokerThread brokerThread = new BrokerThread() {
                private Pair<Integer, String> queryResponse = null;
                public void run() {
                    QueryPlan queryPlan = new KVQueryPlanSumGet(Collections.singletonList(finalI));
                    this.queryResponse = broker.readQuery(queryPlan);
                }
                public Pair<Integer, String> getQueryResponse() {
                    return this.queryResponse;
                }
            };
            brokerThread.start();
            brokerThreads.add(brokerThread);
        }
        for (int i = 0; i < 100; i++) {
            BrokerThread brokerThread = brokerThreads.get(i);
            brokerThread.join();
            Pair<Integer, String> queryResponse = brokerThread.getQueryResponse();
            assertEquals(Integer.valueOf(0), queryResponse.getValue0());
            assertEquals(Integer.toString(i), queryResponse.getValue1());
        }
        for (int i = 0; i < num_datastores; i++) {
            dataStores.get(i).stopServing();
        }
        coordinator.stopServing();
        broker.shutdown();
    }
}

abstract class BrokerThread extends Thread {
    public abstract Pair<Integer, String> getQueryResponse();
}

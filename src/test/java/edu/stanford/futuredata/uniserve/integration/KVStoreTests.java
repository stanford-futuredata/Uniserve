package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.*;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class KVStoreTests {

    private static final Logger logger = LoggerFactory.getLogger(KVStoreTests.class);

    private static String zkHost = "127.0.0.1";
    private static Integer zkPort = 2181;

    public static void cleanUp(String zkHost, int zkPort) {
        // Clean up ZooKeeper
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        try {
            for (String child : cf.getChildren().forPath("/")) {
                if (!child.equals("zookeeper")) {
                    cf.delete().deletingChildrenIfNeeded().forPath("/" + child);
                }
            }
        } catch (Exception e) {
            logger.info("Zookeeper cleanup failed: {}", e.getMessage());
        }
        // Clean up directories.
        try {
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve"));
        } catch (IOException e) {
            logger.info("FS cleanpu failed: {}", e.getMessage());
        }
    }

    @AfterEach
    private void unitTestCleanUp() throws Exception {
        cleanUp(zkHost, zkPort);
    }

    @Test
    public void testSingleKey() {
        logger.info("testSingleKey");
        int numShards = 1;
        Coordinator coordinator = new Coordinator(zkHost, zkPort, "127.0.0.1", 7777);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        DataStore<KVRow, KVShard>  dataStore = new DataStore<>(null, new KVShardFactory(),
                Path.of("/var/tmp/KVUniserve"), zkHost, zkPort, "127.0.0.1", 8000);
        int d_r = dataStore.startServing();
        assertEquals(0, d_r);
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), numShards);

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(1, 2)));
        assertTrue(writeSuccess);

        ReadQueryPlan<KVShard, Integer, Integer> readQueryPlan = new KVReadQueryPlanGet(1);
        Integer queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(2), queryResponse);

        dataStore.shutDown();
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testMultiKey() {
        logger.info("testMultiKey");
        int numShards = 2;
        Coordinator coordinator = new Coordinator(zkHost, zkPort, "127.0.0.1", 7778);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int num_datastores = 4;
        for (int i = 0; i < num_datastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(null, new KVShardFactory(),
                    Path.of("/var/tmp/KVUniserve"), zkHost, zkPort,"127.0.0.1",  8100 + i);
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

        readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 5));
        queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(6), queryResponse);

        readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(55), queryResponse);

        for (int i = 0; i < num_datastores; i++) {
           dataStores.get(i).shutDown();
        }
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testBasicNestedQuery() {
        logger.info("testBasicNestedQuery");
        int numShards = 1;
        Coordinator coordinator = new Coordinator(zkHost, zkPort, "127.0.0.1", 7777);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        DataStore<KVRow, KVShard> dataStore =
                new DataStore<>(null,
                        new KVShardFactory(), Path.of("/var/tmp/KVUniserve"),
                        zkHost, zkPort, "127.0.0.1", 8000);
        int d_r = dataStore.startServing();
        assertEquals(0, d_r);
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), numShards);

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(0, 1)));
        assertTrue(writeSuccess);
        writeSuccess = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(1, 2)));
        assertTrue(writeSuccess);

        ReadQueryPlan<KVShard, Integer, Integer> readQueryPlan = new KVReadQueryPlanNested(0);
        Integer queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(2), queryResponse);

        dataStore.shutDown();
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testSimultaneousReadQuery() throws InterruptedException {
        logger.info("testSimultaneousReadQuery");
        int numShards = 10;
        Coordinator coordinator = new Coordinator(zkHost, zkPort, "127.0.0.1", 7779);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int num_datastores = 4;
        for (int i = 0; i < num_datastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8200 + i);
            int d_r = dataStore.startServing();
            assertEquals(0, d_r);
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), numShards);

        List<KVRow> rows = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            rows.add(new KVRow(i, i));
        }
        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, rows);
        assertTrue(writeSuccess);

        Thread.sleep(2000);
        List<BrokerThread> brokerThreads = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            BrokerThread brokerThread = new BrokerThread() {
                private Integer queryResponse = null;
                public void run() {
                    ReadQueryPlan<KVShard, Integer, Integer> readQueryPlan = new KVReadQueryPlanSumGet(Collections.singletonList(finalI));
                    this.queryResponse = broker.readQuery(readQueryPlan);
                }
                public Integer getQueryResponse() {
                    return this.queryResponse;
                }
            };
            brokerThread.start();
            brokerThreads.add(brokerThread);
        }
        for (int i = 0; i < 100; i++) {
            BrokerThread brokerThread = brokerThreads.get(i);
            brokerThread.join();
            Integer queryResponse = brokerThread.getQueryResponse();
            assertEquals(Integer.valueOf(i), queryResponse);
        }
        for (int i = 0; i < num_datastores; i++) {
            dataStores.get(i).shutDown();
        }
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testShardUpload() {
        logger.info("testShardUpload");
        int numShards = 1;
        Coordinator coordinator = new Coordinator(zkHost, zkPort, "127.0.0.1", 7777);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        DataStore<KVRow, KVShard> dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                new KVShardFactory(), Path.of("/var/tmp/KVUniserve"),
                zkHost, zkPort, "127.0.0.1", 8000);
        int d_r = dataStore.startServing();
        assertEquals(0, d_r);
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), numShards);

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(1, 2)));
        assertTrue(writeSuccess);

        Optional<Pair<String, Integer>> uploadResult = dataStore.uploadShardToCloud(0);
        assertTrue(uploadResult.isPresent());
        String cloudName = uploadResult.get().getValue0();
        Integer shardVersion = uploadResult.get().getValue1();
        assertTrue(shardVersion >= 1);
        Optional<KVShard> shard = dataStore.downloadShardFromCloud(0, cloudName, 1);
        assertTrue(shard.isPresent());

        dataStore.shutDown();
        coordinator.stopServing();
        broker.shutdown();
    }
}

abstract class BrokerThread extends Thread {
    public abstract Integer getQueryResponse();
}

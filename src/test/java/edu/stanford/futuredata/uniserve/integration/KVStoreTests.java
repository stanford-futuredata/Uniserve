package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVQueryEngine;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShardFactory;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.*;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;


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
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve0"));
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve1"));
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve2"));
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve3"));
        } catch (IOException e) {
            logger.info("FS cleanup failed: {}", e.getMessage());
        }
    }

    @BeforeAll
    static void startUpCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @AfterEach
    private void unitTestCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @Test
    public void testSingleKey() {
        logger.info("testSingleKey");
        int numShards = 1;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        DataStore<KVRow, KVShard>  dataStore = new DataStore<>(null, new KVShardFactory(), Path.of("/var/tmp/KVUniserve"), zkHost, zkPort, "127.0.0.1", 8000, -1, false
        );
        dataStore.startServing();
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        assertTrue(broker.createTable("table1", numShards));
        assertTrue(broker.createTable("table2", numShards));
        assertFalse(broker.createTable("table1", numShards + 1));

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert("table1");
        assertTrue(broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(1, 2))));

        AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet("table1", 1);
        assertEquals(Integer.valueOf(2), broker.anchoredReadQuery(readQueryPlan));

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan2 = new KVWriteQueryPlanInsert("table2");
        assertTrue(broker.writeQuery(writeQueryPlan2, Collections.singletonList(new KVRow(1, 3))));

        AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan2 = new KVReadQueryPlanGet("table2", 1);
        assertEquals(Integer.valueOf(3), broker.anchoredReadQuery(readQueryPlan2));

        dataStore.shutDown();
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testMultiKey() {
        logger.info("testMultiKey");
        int numShards = 2;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7778);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(null, new KVShardFactory(), Path.of("/var/tmp/KVUniserve"), zkHost, zkPort, "127.0.0.1", 8100 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);
        List<KVRow> rows = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            rows.add(new KVRow(i, i));
        }
        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, rows);
        assertTrue(writeSuccess);

        AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanSumGet(Collections.singletonList(1));
        Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(Integer.valueOf(1), queryResponse);

        readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 5));
        queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(Integer.valueOf(6), queryResponse);

        readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(Integer.valueOf(55), queryResponse);

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testMultiStageQuery() {
        logger.info("testMultiStageQuery");
        int numShards = 2;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7778);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(null, new KVShardFactory(), Path.of("/var/tmp/KVUniserve"), zkHost, zkPort, "127.0.0.1", 8100 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);
        List<KVRow> rows = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            rows.add(new KVRow(i, i));
        }
        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, rows);
        assertTrue(writeSuccess);

        AnchoredReadQueryPlan<KVShard, Map<String, Map<Integer, Integer>>> readQueryPlan =
                new KVIntermediateSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        Map<String, Map<Integer, Integer>> queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(1, queryResponse.size());
        assertEquals(numShards, queryResponse.get("intermediate1").size());

        AnchoredReadQueryPlan<KVShard, Integer> p = new KVFilterSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        Integer ret = broker.anchoredReadQuery(p);
        assertEquals(10, ret);

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testAddingServers() throws InterruptedException {
        logger.info("testAddingServers");
        int numShards = 5;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7778);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        DataStore<KVRow, KVShard>  dataStoreOne = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", 1)), zkHost, zkPort, "127.0.0.1", 8200, -1, false
        );
        dataStoreOne.runPingDaemon = false;
        dataStoreOne.startServing();

        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);
        List<KVRow> rows = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            rows.add(new KVRow(i, i));
        }
        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        assertTrue(broker.writeQuery(writeQueryPlan, rows));

        DataStore<KVRow, KVShard>  dataStoreTwo = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", 2)), zkHost, zkPort, "127.0.0.1", 8201, -1, false
        );
        dataStoreTwo.runPingDaemon = false;
        assertTrue(dataStoreTwo.startServing());

        Thread.sleep(Broker.shardMapDaemonSleepDurationMillis * 2);

        assertTrue(broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(1, 2))));

        AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanSumGet(Collections.singletonList(1));
        Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(Integer.valueOf(2), queryResponse);

        readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 5));
        queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(Integer.valueOf(7), queryResponse);

        readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(Integer.valueOf(56), queryResponse);

        dataStoreOne.shutDown();
        dataStoreTwo.shutDown();
        coordinator.stopServing();
        broker.shutdown();
    }


    @Test
    public void testBroadcastJoin() {
        logger.info("testBroadcastJoin");
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)), zkHost, zkPort, "127.0.0.1", 8200 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        assertTrue(broker.createTable("table1", 2));
        assertTrue(broker.createTable("table2", 1));

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert("table1");
        assertTrue(broker.writeQuery(writeQueryPlan, List.of(new KVRow(1, 1), new KVRow(2, 2))));

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan2 = new KVWriteQueryPlanInsert("table2");
        assertTrue(broker.writeQuery(writeQueryPlan2, List.of(new KVRow(1, 4), new KVRow(2, 5))));

        AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan2 = new KVPseudoBroadcastJoin("table1", "table2");
        assertEquals(Integer.valueOf(12), broker.anchoredReadQuery(readQueryPlan2));

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    static abstract class BrokerThread extends Thread {
        public abstract Integer getQueryResponse();
    }

    @Test
    public void testSimultaneousReadQuery() throws InterruptedException {
        logger.info("testSimultaneousReadQuery");
        int numShards = 5;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)), zkHost, zkPort, "127.0.0.1", 8200 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);

        List<KVRow> rows = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            rows.add(new KVRow(i, i));
        }
        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, rows);
        assertTrue(writeSuccess);

        List<BrokerThread> brokerThreads = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            BrokerThread brokerThread = new BrokerThread() {
                private Integer queryResponse = null;
                public void run() {
                    AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanSumGet(Collections.singletonList(finalI));
                    this.queryResponse = broker.anchoredReadQuery(readQueryPlan);
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
        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testReplication() {
        logger.info("testReplication");
        int numShards = 5;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard>> dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)), zkHost, zkPort, "127.0.0.1", 8200 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);
        for (int i = 1; i < 6; i++) {
            WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
            boolean writeSuccess = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(i, i)));
            assertTrue(writeSuccess);
            AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(i), queryResponse);
        }

        coordinator.addReplica(0, 1);
        coordinator.addReplica(1, 2);
        coordinator.addReplica(2, 3);
        coordinator.addReplica(3, 2);
        coordinator.addReplica(3, 0);
        coordinator.addReplica(3, 1);
        for (int i = 1; i < 6; i++) {
            WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
            boolean writeSuccess = broker.writeQuery(writeQueryPlan, Arrays.asList(new KVRow(i, 2 * i),
                    new KVRow(2 * i, 4 * i), new KVRow(4 * i, 8 * i)));
            assertTrue(writeSuccess);
            AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(2 * i), queryResponse);
            readQueryPlan = new KVReadQueryPlanGet(2 * i);
            queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(4 * i), queryResponse);
            readQueryPlan = new KVReadQueryPlanGet(4 *  i);
            queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(8 * i), queryResponse);
        }

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

     @Test
    public void testAddRemoveReplicas() throws InterruptedException {
        logger.info("testAddRemoveReplicas");
        int numShards = 5;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)), zkHost, zkPort, "127.0.0.1", 8200 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);
        for (int i = 1; i < 6; i++) {
            WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
            boolean writeSuccess = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(i, i)));
            assertTrue(writeSuccess);
            AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(i), queryResponse);
        }

        coordinator.addReplica(0, 1);
        coordinator.addReplica(1, 2);
        coordinator.addReplica(2, 3);
        coordinator.addReplica(3, 2);
        coordinator.addReplica(3, 0);
        coordinator.addReplica(3, 1);
        coordinator.addReplica(3, 1);

        for (int i = 1; i < 6; i++) {
            WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
            boolean writeSuccess = broker.writeQuery(writeQueryPlan, Arrays.asList(new KVRow(i, 2 * i),
                    new KVRow(2 * i, 4 * i), new KVRow(4 * i, 8 * i)));
            assertTrue(writeSuccess);
            AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(2 * i), queryResponse);
            readQueryPlan = new KVReadQueryPlanGet(2 * i);
            queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(4 * i), queryResponse);
            readQueryPlan = new KVReadQueryPlanGet(4 *  i);
            queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(8 * i), queryResponse);
        }

        coordinator.removeShard(0, 1);
        coordinator.addReplica(0, 2);
        coordinator.removeShard(1, 1);
        coordinator.removeShard(3, 0);
        coordinator.removeShard(3, 3);
        coordinator.removeShard(3, 1);
        coordinator.addReplica(3, 3);
        coordinator.addReplica(0, 2);

        for (int i = 1; i < 6; i++) {
            WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
            boolean writeSuccess = broker.writeQuery(writeQueryPlan, Arrays.asList(new KVRow(i, 2 * i + 1),
                    new KVRow(2 * i, 4 * i + 1), new KVRow(4 * i, 8 * i + 1)));
            assertTrue(writeSuccess);
            AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(2 * i + 1), queryResponse);
            readQueryPlan = new KVReadQueryPlanGet(2 * i);
            queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(4 * i + 1), queryResponse);
            readQueryPlan = new KVReadQueryPlanGet(4 *  i);
            queryResponse = broker.anchoredReadQuery(readQueryPlan);
            assertEquals(Integer.valueOf(8 * i + 1), queryResponse);
        }
        coordinator.removeShard(2, 3);
        AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(2);
        Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(Integer.valueOf(2 * 2 + 1), queryResponse);


        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testAbortedWrite() {
        logger.info("testAbortedWrite");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        DataStore<KVRow, KVShard>  dataStore = new DataStore<>(null, new KVShardFactory(), Path.of("/var/tmp/KVUniserve"), zkHost, zkPort, "127.0.0.1", 8000, -1, false
        );
        dataStore.startServing();
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, List.of(new KVRow(1, 1), new KVRow(2, 2), new KVRow(3, 2)));
        assertTrue(writeSuccess);

        writeSuccess = broker.writeQuery(writeQueryPlan, List.of(new KVRow(1, 3), new KVRow(2, 3), new KVRow(3, 3),
                new KVRow(123123123, 1)));
        assertFalse(writeSuccess);

        AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 2, 3));
        Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
        assertEquals(Integer.valueOf(5), queryResponse);

        dataStore.shutDown();
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testShardUpload() {
        logger.info("testShardUpload");
        int numShards = 1;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        DataStore<KVRow, KVShard> dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                new KVShardFactory(), Path.of("/var/tmp/KVUniserve"), zkHost, zkPort, "127.0.0.1", 8000, -1, false
        );
        dataStore.startServing();
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);

        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(1, 2)));
        assertTrue(writeSuccess);

        Optional<KVShard> shard = dataStore.downloadShardFromCloud(0, "0_1", 1);
        assertTrue(shard.isPresent());

        dataStore.shutDown();
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testSimultaneousWrites() throws InterruptedException {
        logger.info("testSimultaneousWrites");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(null,
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)), zkHost, zkPort, "127.0.0.1", 8200 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);

        List<Thread> threads = new ArrayList<>();
        int numThreads = 10;

        long startTime = System.currentTimeMillis();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            int finalThreadNum = threadNum;
            Thread t = new Thread(() -> {
                while (System.currentTimeMillis() < startTime + 1000) {
                    List<KVRow> insertList = new ArrayList<>();
                    for (int i = 0; i < numShards; i++) {
                        insertList.add(new KVRow(i, finalThreadNum));
                    }
                    WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
                    assertTrue(broker.writeQuery(writeQueryPlan, insertList));
                }
            });
            t.start();
            threads.add(t);
        }

        for(Thread t: threads) {
            t.join();
        }

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testSimultaneousWritesReplicas() throws InterruptedException {
        logger.info("testSimultaneousWritesReplicas");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = numShards;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)), zkHost, zkPort, "127.0.0.1", 8200 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);

        List<Thread> threads = new ArrayList<>();
        int numThreads = 10;

        List<KVRow> firstList = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            firstList.add(new KVRow(i, 0));
        }
        WriteQueryPlan<KVRow, KVShard> firstPlan = new KVWriteQueryPlanInsert();
        assertTrue(broker.writeQuery(firstPlan, firstList));

        for (int i = 0; i < numShards; i++) {
            coordinator.addReplica(i, (i + 1) % numShards);
        }

        long startTime = System.currentTimeMillis();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            int finalThreadNum = threadNum;
            Thread t = new Thread(() -> {
                while (System.currentTimeMillis() < startTime + 1000) {
                    List<KVRow> insertList = new ArrayList<>();
                    for (int i = 0; i < numShards; i++) {
                        insertList.add(new KVRow(i, finalThreadNum));
                    }
                    WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
                    assertTrue(broker.writeQuery(writeQueryPlan, insertList));
                }
            });
            t.start();
            threads.add(t);
        }

        for(Thread t: threads) {
            t.join();
        }

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testReadWriteAtomicity() throws InterruptedException {
        logger.info("testReadWriteAtomicity");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8200 + i, -1, true
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);

        List<Thread> threads = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        for (int threadNum = 0; threadNum < 2; threadNum++) {
            int finalThreadNum = threadNum;
            Thread t = new Thread(() -> {
                while (System.currentTimeMillis() < startTime + 10000) {
                    List<KVRow> insertList = new ArrayList<>();
                    for (int i = 0; i < numShards; i++) {
                        insertList.add(new KVRow(i, finalThreadNum));
                    }
                    WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsertSlow();
                    assertTrue(broker.writeQuery(writeQueryPlan, insertList));
                }
            });
            t.start();
            threads.add(t);
        }

        Thread.sleep(500);

        for (int threadNum = 0; threadNum < 20; threadNum++) {
            Thread t = new Thread(() -> {
                while (System.currentTimeMillis() < startTime + 10000) {
                    KVReadQueryPlanSumGet p = new KVReadQueryPlanSumGet(IntStream.range(0, numShards).boxed().collect(Collectors.toList()));
                    Integer bob = broker.anchoredReadQuery(p);
                    assertEquals(0, bob % numShards);
                }
            });
            t.start();
            threads.add(t);
        }

        for(Thread t: threads) {
            t.join();
        }

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testReadWriteAtomicityReplicas() throws InterruptedException {
        logger.info("testReadWriteAtomicityReplicas");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = numShards;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8200 + i, -1, true
            );
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);

        List<Thread> threads = new ArrayList<>();

        List<KVRow> firstList = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            firstList.add(new KVRow(i, 0));
        }
        WriteQueryPlan<KVRow, KVShard> firstPlan = new KVWriteQueryPlanInsert();
        assertTrue(broker.writeQuery(firstPlan, firstList));

        for (int i = 0; i < numShards; i++) {
            coordinator.addReplica(i, (i + 1) % numShards);
        }

        long startTime = System.currentTimeMillis();
        for (int threadNum = 0; threadNum < 2; threadNum++) {
            int finalThreadNum = threadNum;
            Thread t = new Thread(() -> {
                while (System.currentTimeMillis() < startTime + 10000) {
                    List<KVRow> insertList = new ArrayList<>();
                    for (int i = 0; i < numShards; i++) {
                        insertList.add(new KVRow(i, finalThreadNum));
                    }
                    WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsertSlow();
                    assertTrue(broker.writeQuery(writeQueryPlan, insertList));
                }
            });
            t.start();
            threads.add(t);
        }

        Thread.sleep(500);

        for (int threadNum = 0; threadNum < 20; threadNum++) {
            Thread t = new Thread(() -> {
                while (System.currentTimeMillis() < startTime + 10000) {
                    KVReadQueryPlanSumGet p = new KVReadQueryPlanSumGet(IntStream.range(0, numShards).boxed().collect(Collectors.toList()));
                    Integer bob = broker.anchoredReadQuery(p);
                    assertEquals(0, bob % numShards);
                }
            });
            t.start();
            threads.add(t);
        }

        for(Thread t: threads) {
            t.join();
        }

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }
}

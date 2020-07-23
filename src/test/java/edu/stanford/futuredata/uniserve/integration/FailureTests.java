package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVQueryEngine;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShardFactory;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVReadQueryPlanGet;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVReadQueryPlanSumGet;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVWriteQueryPlanInsert;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVWriteQueryPlanInsertSlow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FailureTests {
    private static final Logger logger = LoggerFactory.getLogger(FailureTests.class);

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

//    @Test
    public void testSingleFailure() throws InterruptedException {
        logger.info("testSingleFailure");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, zkHost, zkPort, "127.0.0.1", 7778);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"), new KVShardFactory(),
                    Path.of(String.format("/var/tmp/KVUniserve%d", 1)), zkHost, zkPort,"127.0.0.1",  8100 + i, -1);
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

        dataStores.get(0).shutDown();

        ReadQueryPlan<KVShard, Integer>  readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        Integer queryResponse;
        do {
            queryResponse = broker.readQuery(readQueryPlan);
        } while (Objects.isNull(queryResponse));
        assertEquals(queryResponse, 55);

        for (int i = 0; i < numDatastores; i++) {
            dataStores.get(i).runPingDaemon = false;
        }
        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testBrokerFailDuringWrite() throws InterruptedException {
        logger.info("testBrokerFailDuringWrite");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDataStores = 4;
        for (int i = 0; i < numDataStores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8200 + i, -1);
            dataStore.runPingDaemon = false;
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);
        Thread t = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsertSlow();
                List<KVRow> rows = new ArrayList<>();
                for(int d = 0; d < numDataStores; d++) {
                    rows.add(new KVRow(d, i % 2));
                }
                boolean writeSuccess = broker.writeQuery(writeQueryPlan, rows);
                for (int shardNum = 0; shardNum < numShards; shardNum++) {
                    int dsID = coordinator.consistentHash.getBucket(shardNum);
                    KVShard s = dataStores.get(dsID).primaryShardMap.get(shardNum);
                    if (writeSuccess) {
                        assertEquals(Optional.of(i % 2), s.queryKey(shardNum));
                    } else {
                        assertEquals(dataStores.get(coordinator.consistentHash.getBucket(0)).primaryShardMap.get(0).queryKey(0), s.queryKey(shardNum));
                    }
                }
            }
        });
        t.start();
        Thread.sleep(2000);
        broker.shutdown();
        t.join();
        Thread.sleep(500);
        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testDataStoreFailDuringWrite() throws InterruptedException {
        logger.info("testDataStoreFailDuringWrite");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDataStores = 2 * numShards;
        for (int i = 0; i < numDataStores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8800 + i, -1);
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);
        List<KVRow> startRows = new ArrayList<>();
        for(int d = 0; d < numShards; d++) {
            startRows.add(new KVRow(d, 0));
        }
        WriteQueryPlan<KVRow, KVShard> q = new KVWriteQueryPlanInsert();
        assertTrue(broker.writeQuery(q, startRows));
        Thread t = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsertSlow();
                List<KVRow> rows = new ArrayList<>();
                for(int d = 0; d < numShards; d++) {
                    rows.add(new KVRow(d, i % 2));
                }
                boolean writeSuccess = broker.writeQuery(writeQueryPlan, rows);
                for (int shardNum = 0; shardNum < numShards; shardNum++) {
                    int dsID = coordinator.consistentHash.getBucket(shardNum);
                    KVShard s = dataStores.get(dsID).primaryShardMap.get(shardNum);
                    if (!Objects.isNull(s)) {
                        if (writeSuccess) {
                            assertEquals(Optional.of(i % 2), s.queryKey(shardNum));
                        } else {
                            assertEquals(dataStores.get(coordinator.consistentHash.getBucket(1)).primaryShardMap.get(1).queryKey(1), s.queryKey(shardNum));
                        }
                    }
                }
            }
        });
        t.start();
        Thread.sleep(250);
        dataStores.get(6).shutDown();
        Thread.sleep(5000);
        t.join();
        for (int i = 0; i < numDataStores; i++) {
            dataStores.get(i).runPingDaemon = false;
        }
        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }

//    @Test
    public void testFailureWithChangingReplicas() {
        logger.info("testFailureWithChangingReplicas");
        int numShards = 5;
        Coordinator coordinator = new Coordinator(null, zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDatastores = 4;
        for (int i = 0; i < numDatastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8200 + i, -1);
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", numShards);
        for (int i = 1; i < 100; i++) {
            WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
            boolean writeSuccess = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(i, i)));
            assertTrue(writeSuccess);
            ReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.readQuery(readQueryPlan);
            assertEquals(Integer.valueOf(i), queryResponse);
        }
        for(DataStore<KVRow, KVShard> dataStore: dataStores) {
            for(int shardNum: dataStore.primaryShardMap.keySet()) {
                dataStore.uploadShardToCloud(shardNum);
            }
        }
        coordinator.addReplica(0, 1);
        coordinator.addReplica(1, 2);
        coordinator.addReplica(2, 3);
        coordinator.addReplica(3, 2);
        dataStores.get(0).shutDown();
        coordinator.addReplica(3, 0);
        coordinator.addReplica(3, 1);
        coordinator.addReplica(3, 1);
        dataStores.get(1).shutDown();

        for (int i = 1; i < 100; i++) {
            ReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.readQuery(readQueryPlan);
            assertTrue(Objects.isNull(queryResponse) || Integer.valueOf(i).equals(queryResponse));
        }

        coordinator.removeShard(0, 1);
        coordinator.addReplica(0, 2);
        coordinator.removeShard(1, 1);
        coordinator.removeShard(3, 0);
        coordinator.removeShard(3, 3);
        dataStores.get(2).shutDown();
        coordinator.removeShard(3, 1);
        coordinator.addReplica(3, 3);
        coordinator.addReplica(0, 2);

        for (int i = 1; i < 100; i++) {
            ReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.readQuery(readQueryPlan);
            assertTrue(Objects.isNull(queryResponse) || Integer.valueOf(i).equals(queryResponse));
        }
        coordinator.removeShard(2, 3);
        ReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(2);
        Integer queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(2), queryResponse);

        for (int i = 0; i < numDatastores; i++) {
            dataStores.get(i).runPingDaemon = false;
        }
        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
    }
}

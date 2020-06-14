package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    public void testSingleFailure() throws InterruptedException {
        logger.info("testSingleFailure");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, zkHost, zkPort, "127.0.0.1", 7778);
        coordinator.runLoadBalancerDaemon = false;
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int num_datastores = 4;
        for (int i = 0; i < num_datastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"), new KVShardFactory(),
                    Path.of("/var/tmp/KVUniserve"), zkHost, zkPort,"127.0.0.1",  8100 + i, -1);
            dataStore.runUploadShardDaemon = false;
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

        for(DataStore<KVRow, KVShard> dataStore: dataStores) {
            for(int shardNum: dataStore.primaryShardMap.keySet()) {
                dataStore.uploadShardToCloud(shardNum);
            }
        }

        dataStores.get(0).shutDown();

        ReadQueryPlan<KVShard, Integer>  readQueryPlan = new KVReadQueryPlanSumGet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        Integer queryResponse;
        do {
            queryResponse = broker.readQuery(readQueryPlan);
        } while (Objects.isNull(queryResponse));
        assertEquals(queryResponse, 55);

        for (int i = 0; i < num_datastores; i++) {
            dataStores.get(i).runPingDaemon = false;
        }
        for (int i = 0; i < num_datastores; i++) {
            dataStores.get(i).shutDown();
        }
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testWriteFailures() throws InterruptedException {
        logger.info("testWriteFailures");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int numDataStores = 4;
        for (int i = 0; i < numDataStores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8200 + i, -1);
            dataStore.runUploadShardDaemon = false;
            dataStore.runPingDaemon = false;
            int d_r = dataStore.startServing();
            assertEquals(0, d_r);
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), numShards);
        Thread t = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsertSlow();
                List<KVRow> rows = new ArrayList<>();
                for(int d = 0; d < numDataStores; d++) {
                    rows.add(new KVRow(d, i % 2));
                }
                boolean writeSuccess = broker.writeQuery(writeQueryPlan, rows);
                int num = 0;
                for (DataStore<KVRow, KVShard> d: dataStores) {
                    KVShard s = d.primaryShardMap.get(num);
                    if (writeSuccess) {
                        assertEquals(Optional.of(i % 2), s.queryKey(num));
                    } else {
                        assertEquals(dataStores.get(0).primaryShardMap.get(0).queryKey(0), s.queryKey(num));
                    }
                    num += 1;
                }
            }
        });
        t.start();
        Thread.sleep(500);
//        dataStores.get(0).shutDown();
        t.join();
        for (int i = 0; i < numDataStores; i++) {
            dataStores.get(i).shutDown();
        }
        coordinator.stopServing();
        broker.shutdown();
    }

    @Test
    public void testFailureWithChangingReplicas() {
        logger.info("testFailureWithChangingReplicas");
        int numShards = 5;
        Coordinator coordinator = new Coordinator(null, zkHost, zkPort, "127.0.0.1", 7779);
        coordinator.runLoadBalancerDaemon = false;
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        List<DataStore<KVRow, KVShard> > dataStores = new ArrayList<>();
        int num_datastores = 4;
        for (int i = 0; i < num_datastores; i++) {
            DataStore<KVRow, KVShard>  dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"),
                    new KVShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)),
                    zkHost, zkPort, "127.0.0.1", 8200 + i, -1);
            dataStore.runUploadShardDaemon = false;
            int d_r = dataStore.startServing();
            assertEquals(0, d_r);
            dataStores.add(dataStore);
        }
        final Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), numShards);
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
        coordinator.addReplica(0, 1, 0.1);
        coordinator.addReplica(1, 2, 0.1);
        coordinator.addReplica(2, 3, 0.1);
        coordinator.addReplica(3, 2, 0.1);
        dataStores.get(0).shutDown();
        coordinator.addReplica(3, 0, 0.1);
        coordinator.addReplica(3, 1, 0.1);
        coordinator.addReplica(3, 1, 0.2);
        dataStores.get(1).shutDown();

        for (int i = 1; i < 100; i++) {
            ReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.readQuery(readQueryPlan);
            assertTrue(Objects.isNull(queryResponse) || Integer.valueOf(i).equals(queryResponse));
        }

        coordinator.removeShard(0, 1);
        coordinator.addReplica(0, 2, 0.1);
        coordinator.removeShard(1, 1);
        coordinator.removeShard(3, 0);
        coordinator.removeShard(3, 3);
        dataStores.get(2).shutDown();
        coordinator.removeShard(3, 1);
        coordinator.addReplica(3, 3, 0.1);
        coordinator.addReplica(0, 2, 0.2);

        for (int i = 1; i < 100; i++) {
            ReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(i);
            Integer queryResponse = broker.readQuery(readQueryPlan);
            assertTrue(Objects.isNull(queryResponse) || Integer.valueOf(i).equals(queryResponse));
        }
        coordinator.removeShard(2, 3);
        ReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(2);
        Integer queryResponse = broker.readQuery(readQueryPlan);
        assertEquals(Integer.valueOf(2), queryResponse);

        for (int i = 0; i < num_datastores; i++) {
            dataStores.get(i).runPingDaemon = false;
        }
        for (int i = 0; i < num_datastores; i++) {
            dataStores.get(i).shutDown();
        }
        coordinator.stopServing();
        broker.shutdown();
    }
}

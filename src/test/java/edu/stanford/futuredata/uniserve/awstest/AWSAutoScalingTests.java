package edu.stanford.futuredata.uniserve.awstest;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.util.EC2MetadataUtils;
import edu.stanford.futuredata.uniserve.awscloud.AWSCoordinatorCloud;
import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.coordinator.*;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShardFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AWSAutoScalingTests {
    private static final Logger logger = LoggerFactory.getLogger(AWSAutoScalingTests.class);

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

    // @Test
    public void testBasicAutoScaling() throws InterruptedException {
        logger.info("testBalanceLoadFunction");
        String serverHost = EC2MetadataUtils.getPrivateIpAddress();
        String launchDataStoreScript =
                "#!/bin/bash\n" +
                "cd /home/ubuntu/Uniserve\n" +
                "nohup java -jar target/Uniserve-1.0-SNAPSHOT-fat-tests.jar -datastore -zh SERVERHOST -zp 2181 -h aws -p 8000 -c CLOUDID > /home/ubuntu/datastore.log &\n" +
                "chmod +444 /home/ubuntu/datastore.log\n";
        launchDataStoreScript = launchDataStoreScript.replace("SERVERHOST", serverHost);
        String ami = "ami-032d8d51f6f913bb5";
        InstanceType instanceType = InstanceType.T2Micro;
        CoordinatorCloud cCloud = new AWSCoordinatorCloud(ami, launchDataStoreScript, instanceType);
        AutoScaler autoScaler = new DefaultAutoScaler();
        Coordinator coordinator = new Coordinator(cCloud, new DefaultLoadBalancer(), autoScaler, zkHost, zkPort, serverHost, 7777);
        coordinator.runLoadBalancerDaemon = false;
        assertTrue(coordinator.startServing());
        DataStore<KVRow, KVShard> dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"), new KVShardFactory(),
                Path.of("/var/tmp/KVUniserve","shard"), zkHost, zkPort,"127.0.0.1",  8300, -1);
        assertTrue(dataStore.startServing());

        Map<Integer, Double> overLoadedMap = Map.of(0, 0.9);
        assertEquals(AutoScaler.ADD, autoScaler.autoscale(overLoadedMap));
        coordinator.addDataStore();

        Thread.sleep(120000);

        Map<Integer, Double> underLoadedMap = Map.of(0, 0.2, 1, 0.2);
        assertEquals(AutoScaler.REMOVE, autoScaler.autoscale(underLoadedMap));
        coordinator.removeDataStore();

        Thread.sleep(5000);

        dataStore.shutDown();
        coordinator.stopServing();
    }
}

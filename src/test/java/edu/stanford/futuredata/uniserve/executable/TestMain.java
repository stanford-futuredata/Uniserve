package edu.stanford.futuredata.uniserve.executable;

import com.amazonaws.util.EC2MetadataUtils;
import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.integration.KVStoreTests;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.*;
import org.apache.commons.cli.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMain {

    private static final Logger logger = LoggerFactory.getLogger(TestMain.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("coordinator", false, "Start Coordinator?");
        options.addOption("broker", false, "Start Broker?");
        options.addOption("datastore", false, "Start Datastore?");

        options.addOption("zh", true, "ZooKeeper Host Address");
        options.addOption("zp", true, "ZooKeeper Port");
        options.addOption("h", true, "Local Host Address");
        options.addOption("p", true, "Local Port");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String serverHost = null;
        if (cmd.hasOption("h")) {
            String hostString = cmd.getOptionValue("h");
            if (hostString.equals("aws")) {
                serverHost = EC2MetadataUtils.getPrivateIpAddress();
            } else {
                serverHost = hostString;
            }
        }

        if (cmd.hasOption("coordinator")) {
            logger.info("Starting coordinator!");
            runCoordinator(cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("zp")),
                    serverHost, Integer.parseInt(cmd.getOptionValue("p")));
        }
        if (cmd.hasOption("datastore")) {
            logger.info("Starting datastore!");
            runDataStore(cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("zp")),
                    serverHost, Integer.parseInt(cmd.getOptionValue("p")));
        }
        if (cmd.hasOption("broker")) {
            logger.info("Starting broker!");
            runBroker(cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("zp")));
        }
    }

    private static void runCoordinator(String zkHost, int zkPort, String cHost, int cPort) throws InterruptedException {
        Coordinator coordinator = new Coordinator(zkHost, zkPort, cHost, cPort);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            coordinator.stopServing();
            KVStoreTests.cleanUp(zkHost, zkPort);
        }));
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void runDataStore(String zkHost, int zkPort, String dsHost, int dsPort) throws InterruptedException {
        DataStore<KVRow, KVShard> dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"), new KVShardFactory(), Path.of("/var/tmp/KVUniserve"), zkHost, zkPort, dsHost, dsPort);
        int d_r = dataStore.startServing();
        assertEquals(0, d_r);
        Runtime.getRuntime().addShutdownHook(new Thread(dataStore::shutDown));
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void runBroker(String zkHost, int zkPort) {
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), 5);
        int addRowReturnCode = broker.insertRow(Collections.singletonList(new KVRow(1, 2)));
        assertEquals(0, addRowReturnCode);
        QueryPlan<KVShard, Integer, Integer> queryPlan = new KVQueryPlanGet(1);
        Integer queryResponse = broker.scheduleQuery(queryPlan);
        assertEquals(Integer.valueOf(2), queryResponse);
        broker.shutdown();
    }
}

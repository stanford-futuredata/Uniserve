package edu.stanford.futuredata.uniserve;

import com.amazonaws.util.EC2MetadataUtils;
import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.integration.KVStoreTests;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVQueryEngine;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShardFactory;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVReadQueryPlanGet;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVWriteQueryPlanInsert;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class KVExecutable {

    private static final Logger logger = LoggerFactory.getLogger(KVExecutable.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("coordinator", false, "Start Coordinator?");
        options.addOption("broker", false, "Start Broker?");
        options.addOption("datastore", false, "Start Datastore?");

        options.addOption("zh", true, "ZooKeeper Host Address");
        options.addOption("zp", true, "ZooKeeper Port");
        options.addOption("h", true, "Local Host Address");
        options.addOption("p", true, "Local Port");
        options.addOption("c", true, "Cloud ID");

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
            int cloudID = cmd.hasOption("c") ? Integer.parseInt(cmd.getOptionValue("c")) : -1;
            logger.info("Starting datastore!");
            runDataStore(cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("zp")),
                    serverHost, Integer.parseInt(cmd.getOptionValue("p")), cloudID);
        }
        if (cmd.hasOption("broker")) {
            logger.info("Starting broker!");
            runBroker(cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("zp")));
        }
    }

    private static void runCoordinator(String zkHost, int zkPort, String cHost, int cPort) throws InterruptedException {
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, cHost, cPort);
        coordinator.startServing();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            coordinator.stopServing();
            KVStoreTests.cleanUp(zkHost, zkPort);
        }));
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void runDataStore(String zkHost, int zkPort, String dsHost, int dsPort, int cloudID) throws InterruptedException {
        DataStore<KVRow, KVShard> dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"), new KVShardFactory(), Path.of("/var/tmp/KVUniserve"), zkHost, zkPort, dsHost, dsPort, cloudID);
        dataStore.startServing();
        Runtime.getRuntime().addShutdownHook(new Thread(dataStore::shutDown));
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void runBroker(String zkHost, int zkPort) {
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine());
        broker.createTable("table", 1);
        WriteQueryPlan<KVRow, KVShard> writeQueryPlan = new KVWriteQueryPlanInsert();
        boolean writeSuccess  = broker.writeQuery(writeQueryPlan, Collections.singletonList(new KVRow(1, 2)));
        assert(writeSuccess);
        for (int iterNum = 0; iterNum < 10; iterNum++) {
            List<Long> trialTimes = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                long t0 = System.nanoTime();
                AnchoredReadQueryPlan<KVShard, Integer> readQueryPlan = new KVReadQueryPlanGet(1);
                Integer queryResponse = broker.anchoredReadQuery(readQueryPlan);
                assert (queryResponse == 2);
                long timeElapsed = System.nanoTime() - t0;
                trialTimes.add(timeElapsed / 1000L);
            }
            List<Long> sortedTimes = trialTimes.stream().sorted().collect(Collectors.toList());
            long average = sortedTimes.stream().mapToLong(i -> i).sum() / trialTimes.size();
            long p50 = sortedTimes.get(trialTimes.size() / 2);
            long p99 = sortedTimes.get(trialTimes.size() * 99 / 100);
            logger.info("Queries: {} Average: {}μs p50: {}μs p99: {}μs", trialTimes.size(), average, p50, p99);
        }
        broker.shutdown();
    }
}

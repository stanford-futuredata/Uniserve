package edu.stanford.futuredata.uniserve.executable;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.*;
import org.apache.commons.cli.*;
import org.javatuples.Pair;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMain {
    public static void main(String[] args) throws ParseException, InterruptedException {
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

        if (cmd.hasOption("coordinator")) {
            System.out.println("Starting coordinator!");
            runCoordinator(cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("zp")),
                    cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("p")));
        }
        if (cmd.hasOption("datastore")) {
            System.out.println("Starting datastore!");
            runDataStore(cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("zp")),
                    cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("p")));
        }
        if (cmd.hasOption("broker")) {
            System.out.println("Starting broker!");
            runBroker(cmd.getOptionValue("zh"), Integer.parseInt(cmd.getOptionValue("zp")));
        }
    }

    private static void runCoordinator(String zkHost, int zkPort, String cHost, int cPort) throws InterruptedException {
        Coordinator coordinator = new Coordinator(zkHost, zkPort, cHost, cPort);
        int c_r = coordinator.startServing();
        assertEquals(0, c_r);
        while(true) {
            Thread.sleep(10);
        }
    }

    private static void runDataStore(String zkHost, int zkPort, String dsHost, int dsPort) throws InterruptedException {
        DataStore dataStore = new DataStore<>(zkHost, zkPort, dsHost, dsPort, new KVShardFactory());
        int d_r = dataStore.startServing();
        assertEquals(0, d_r);
        while(true) {
            Thread.sleep(10);
        }
    }

    private static void runBroker(String zkHost, int zkPort) {
        Broker broker = new Broker(zkHost, zkPort, new KVQueryEngine(), 5);

        int addRowReturnCode = broker.insertRow(new KVRow(1, 2));
        assertEquals(0, addRowReturnCode);

        QueryPlan<KVShard, Integer, Integer> queryPlan = new KVQueryPlanGet(1);
        Pair<Integer, Integer> queryResponse = broker.readQuery(queryPlan);
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals(Integer.valueOf(2), queryResponse.getValue1());
    }
}

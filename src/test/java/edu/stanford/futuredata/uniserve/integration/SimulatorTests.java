package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.coordinator.LoadBalancer;
import edu.stanford.futuredata.uniserve.simulator.Simulator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimulatorTests {
    private static final Logger logger = LoggerFactory.getLogger(SimulatorTests.class);

    @BeforeAll
    static void setup() {
        LoadBalancer.verbose = false;
    }

    @AfterAll
    static void cleanup() {
        LoadBalancer.verbose = true;
    }

    @Test
    public void testBasicSimulator() {
        logger.info("testBasicSimulator");
        int numShards = 40;
        int numServers = 20;
        Simulator simulator = new Simulator(numShards, numServers);
        simulator.run(10000);
    }
}

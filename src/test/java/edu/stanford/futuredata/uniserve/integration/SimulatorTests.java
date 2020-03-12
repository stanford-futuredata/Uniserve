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
        LoadBalancer.verbose = true;
    }

    @AfterAll
    static void cleanup() {
        LoadBalancer.verbose = true;
    }

    @Test
    public void testBasicSimulator() {
        logger.info("testBasicSimulator");
        int numShards = 50;
        int numServers = 5;
        Simulator simulator = new Simulator(numShards, numServers);
        simulator.run(5000000);
    }
}

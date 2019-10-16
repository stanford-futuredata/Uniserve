package edu.stanford.futuredata.uniserve.integration;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class BasicIntegrationTests {

    private static final Logger logger = LoggerFactory.getLogger(BasicIntegrationTests.class);

    @Test
    public void testSimple() {
        assertEquals(5.0, 5.0, 0.1);
        logger.info("bob");
    }
}

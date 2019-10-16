package edu.stanford.futuredata.uniserve.integration;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.KVQueryEngine;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.KVShardFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BasicIntegrationTests {

    private static final Logger logger = LoggerFactory.getLogger(BasicIntegrationTests.class);

    @Test
    public void testSimple() throws IOException {
        DataStore dataStore = new DataStore(8888, new KVShardFactory());
        dataStore.startServing();
        Broker broker = new Broker("127.0.0.1", 8888, new KVQueryEngine());

        int response = broker.addRowQuery(0, ByteString.copyFrom("1 2".getBytes()));

        assertEquals(response, 0);
        dataStore.stopServing();
    }
}

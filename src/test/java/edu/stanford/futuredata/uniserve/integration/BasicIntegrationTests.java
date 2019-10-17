package edu.stanford.futuredata.uniserve.integration;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.KVQueryEngine;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.KVShardFactory;
import org.javatuples.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BasicIntegrationTests {

    private static final Logger logger = LoggerFactory.getLogger(BasicIntegrationTests.class);

    @Test
    public void testSimple() throws IOException {
        int numShards = 1;
        Coordinator coordinator = new Coordinator(7777);
        coordinator.startServing();
        DataStore dataStore = new DataStore(8888, new KVShardFactory());
        dataStore.startServing("localhost", 7777);
        Broker broker = new Broker("127.0.0.1", 8888, new KVQueryEngine(numShards));

        int addRowReturnCode = broker.insertRow(0, ByteString.copyFrom("1 2".getBytes()));
        assertEquals(0, addRowReturnCode);

        Pair<Integer, String> queryResponse = broker.readQuery("1");
        assertEquals(Integer.valueOf(0), queryResponse.getValue0());
        assertEquals("2", queryResponse.getValue1());

        dataStore.stopServing();
        coordinator.stopServing();
    }
}

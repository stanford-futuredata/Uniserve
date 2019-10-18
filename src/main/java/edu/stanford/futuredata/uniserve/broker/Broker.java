package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class Broker {

    private final QueryEngine queryEngine;
    private final BrokerCurator zkCurator;

    private static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    private final BrokerDataStoreGrpc.BrokerDataStoreBlockingStub dataStoreBlockingStub;
    private final BrokerCoordinatorGrpc.BrokerCoordinatorBlockingStub coordinatorBlockingStub;

    private String masterHost = null;
    private Integer masterPort = null;

    public Broker(String zkHost, int zkPort, QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
        this.zkCurator = new BrokerCurator(zkHost, zkPort);
        Optional<Pair<String, Integer>> masterHostPort = zkCurator.getMasterLocation();
        if (masterHostPort.isPresent()) {
            masterHost = masterHostPort.get().getValue0();
            masterPort = masterHostPort.get().getValue1();
        } else {
            logger.error("Broker could not find master");
        }
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(masterHost, masterPort).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        coordinatorBlockingStub = BrokerCoordinatorGrpc.newBlockingStub(channel);

        // TODO:  Replace
        ShardLocationMessage m = ShardLocationMessage.newBuilder().setShard(0).build();
        ShardLocationResponse r = null;
        try {
            r = coordinatorBlockingStub.shardLocation(m);
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed: {}", e.getStatus());
        }
        Pair<String, Integer> hostPort = Utilities.parseConnectString(r.getConnectString());
        ManagedChannelBuilder channelBuilder2 = ManagedChannelBuilder.forAddress(hostPort.getValue0(), hostPort.getValue1()).usePlaintext();
        ManagedChannel channel2 = channelBuilder2.build();
        dataStoreBlockingStub = BrokerDataStoreGrpc.newBlockingStub(channel2);
    }


    public Integer insertRow(int shard, ByteString rowData) {
        InsertRowMessage row = InsertRowMessage.newBuilder().setShard(shard).setRowData(rowData).build();
        InsertRowResponse addRowAck;
        try {
            addRowAck = dataStoreBlockingStub.insertRow(row);
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed: {}", e.getStatus());
            return 1;
        }
        return addRowAck.getReturnCode();
    }

    public Pair<Integer, String> readQuery(String query) {
        QueryPlan queryPlan = queryEngine.planQuery(query);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(queryPlan);
            out.flush();
        } catch (IOException e) {
            logger.warn("Query Serialization Failed: {}", e.getMessage());
            return new Pair<>(1, "");
        }
        ByteString serializedQuery = ByteString.copyFrom(bos.toByteArray());
        ReadQueryMessage readQuery = ReadQueryMessage.newBuilder().setShard(0).setSerializedQuery(serializedQuery).build();
        ReadQueryResponse readQueryResponse;
        try {
            readQueryResponse = dataStoreBlockingStub.readQuery(readQuery);
            assert readQueryResponse.getReturnCode() == 0;
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed: {}", e.getStatus());
            return new Pair<>(1, "");
        }
        List<ByteString> intermediates = Collections.singletonList(readQueryResponse.getResponse());
        String responseString = queryPlan.aggregateShardQueries(intermediates);
        return new Pair<>(0, responseString);
    }
}


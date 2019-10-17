package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
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

public class Broker {

    private final QueryEngine queryEngine;

    private static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    private final QueryDataGrpc.QueryDataBlockingStub blockingStub;

    public Broker(String host, int port, QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        blockingStub = QueryDataGrpc.newBlockingStub(channel);
    }


    public Integer insertRow(int shard, ByteString rowData) {
        InsertRowMessage row = InsertRowMessage.newBuilder().setShard(shard).setRowData(rowData).build();
        InsertRowResponse addRowAck;
        try {
            addRowAck = blockingStub.insertRow(row);
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
            readQueryResponse = blockingStub.readQuery(readQuery);
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


package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.AddRowAck;
import edu.stanford.futuredata.uniserve.QueryDataGrpc;
import edu.stanford.futuredata.uniserve.Row;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public int addRowQuery(int shard, ByteString rowData) {
        Row row = Row.newBuilder().setShard(shard).setRowData(rowData).build();
        AddRowAck addRowAck;
        try {
            addRowAck = blockingStub.addRow(row);
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed: {}", e.getStatus());
            return 1;
        }
        return addRowAck.getReturnCode();
    }
}


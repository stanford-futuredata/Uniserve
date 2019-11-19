package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.CoordinatorDataStoreGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DataStoreDescription {

    public final String host;
    public final int port;
    public final CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub;
    public final String connectString;

    public DataStoreDescription(String host, int port) {
        this.host = host;
        this.port = port;
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        this.stub = CoordinatorDataStoreGrpc.newBlockingStub(channel);
        this.connectString = String.format("%s:%d", host, port);
    }
}

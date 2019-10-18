package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.CoordinatorDataStoreGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DataStoreDescription {

    private final String host;
    private final int port;
    private final CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub;

    public DataStoreDescription(String host, int port) {
        this.host = host;
        this.port = port;
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        this.stub = CoordinatorDataStoreGrpc.newBlockingStub(channel);
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub getStub() {
        return stub;
    }
}

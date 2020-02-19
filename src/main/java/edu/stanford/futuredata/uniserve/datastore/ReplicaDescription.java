package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.DataStoreDataStoreGrpc;
import io.grpc.ManagedChannel;

public class ReplicaDescription {
    public final int dsID;
    public final ManagedChannel channel;
    public final DataStoreDataStoreGrpc.DataStoreDataStoreStub stub;

    public ReplicaDescription(int dsID, ManagedChannel channel, DataStoreDataStoreGrpc.DataStoreDataStoreStub stub) {
        this.dsID = dsID;
        this.channel = channel;
        this.stub = stub;
    }

    @Override
    public String toString() {
        return "ReplicaDescription{" +
                "dsID=" + dsID +
                '}';
    }
}

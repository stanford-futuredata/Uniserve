package edu.stanford.futuredata.uniserve.datastore;

import io.grpc.stub.StreamObserver;

public interface PreemptibleStreamObserver<V> extends StreamObserver<V> {
    public void preempt();

    public void resume();

    public long getTXID();
}

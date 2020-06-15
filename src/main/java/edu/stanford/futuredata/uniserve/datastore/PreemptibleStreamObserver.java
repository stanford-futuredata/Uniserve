package edu.stanford.futuredata.uniserve.datastore;

import io.grpc.stub.StreamObserver;

public interface PreemptibleStreamObserver<V> extends StreamObserver<V> {
    boolean preempt();

    void resume();

    long getTXID();
}

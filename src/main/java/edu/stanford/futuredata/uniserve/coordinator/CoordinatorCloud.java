package edu.stanford.futuredata.uniserve.coordinator;

public interface CoordinatorCloud {
    boolean addDataStore();

    void removeDataStore(int cloudID);

    void shutdown();
}

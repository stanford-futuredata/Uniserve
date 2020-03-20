package edu.stanford.futuredata.uniserve.coordinator;

public interface CoordinatorCloud {
    public boolean addDataStore(int dsID);

    public void removeDataStore(int dsID);
}

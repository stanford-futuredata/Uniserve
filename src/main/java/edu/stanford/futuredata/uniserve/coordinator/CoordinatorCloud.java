package edu.stanford.futuredata.uniserve.coordinator;

import java.util.Optional;

public interface CoordinatorCloud {
    public boolean addDataStore();

    public void removeDataStore(int cloudID);
}

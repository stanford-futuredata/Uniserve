package edu.stanford.futuredata.uniserve.coordinator;

import java.util.Map;
import java.util.Set;

public interface LoadBalancer {
    public Map<Integer, Integer> balanceLoad(Set<Integer> shards, Set<Integer> servers,
                                                    Map<Integer, Integer> shardLoads,
                                                    Map<Integer, Integer> currentLocations);
}

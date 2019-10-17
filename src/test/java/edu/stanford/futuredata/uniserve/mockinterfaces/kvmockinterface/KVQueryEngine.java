package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;

public class KVQueryEngine implements QueryEngine {

    private final int numShards;

    public KVQueryEngine(int numShards) {
        this.numShards = numShards;
    }

    @Override
    public QueryPlan planQuery(String query) {
        Integer key = Integer.parseInt(query);
        return new KVQueryPlan(key);
    }

    @Override
    public int keyToShard(int partitionKey) {
        return partitionKey % this.numShards;
    }
}

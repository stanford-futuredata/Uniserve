package edu.stanford.futuredata.uniserve.interfaces;

import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;

public interface QueryEngine {
    /*
     Lives on a broker.
     Maps partition keys to shards.

     // TODO:  Potentially generates query plans?
     */

    // To which shard should a partition key be assigned?
    public int keyToShard(int partitionKey);
}

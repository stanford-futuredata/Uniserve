package edu.stanford.futuredata.uniserve.interfaces;

import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;

public interface QueryEngine {
    /*
     Lives on a broker.  Generates a query plan (query functions and target keys) for any user-submitted query.
     Maps partition keys to shards.
     */

    // Generate a query plan for a user-submitted query.
    public QueryPlan planQuery(String query);

    // To which shard should a partition key be assigned?
    public int keyToShard(int partitionKey);
}

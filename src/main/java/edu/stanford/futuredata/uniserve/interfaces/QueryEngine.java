package edu.stanford.futuredata.uniserve.interfaces;

public interface QueryEngine {
    /*
     Lives on a broker.
     Maps partition keys to shards.

     // TODO:  Potentially generates query plans?
     */

    // To which shard should a partition key be assigned?
    int keyToShard(int partitionKey);
}

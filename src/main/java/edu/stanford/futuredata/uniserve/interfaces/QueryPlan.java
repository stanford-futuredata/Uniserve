package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface QueryPlan<S extends Shard, I extends Serializable, T> extends Serializable {
    /*
     Execute a query on the shards containing certain keys, then aggregate the result.
     */

    // On which keys should the query execute?
    List<Integer> keysForQuery();
    // Execute the query on an shard (Map).
    I queryShard(S shard);
    // Aggregate the outputs of queries on shards (Reduce).
    T aggregateShardQueries(List<I> shardQueryResults);
}

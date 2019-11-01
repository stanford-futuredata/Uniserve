package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface QueryPlan<T extends Serializable, S> extends Serializable {
    /*
     Execute a query on the shards containing certain keys, then aggregate the result.
     */

    // On which keys should the query execute?
    List<Integer> keysForQuery();
    // Execute the query on an shard (Map).
    T queryShard(Shard shard);
    // Aggregate the outputs of queries on shards (Reduce).
    S aggregateShardQueries(List<T> shardQueryResults);
}

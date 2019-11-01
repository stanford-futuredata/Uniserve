package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface QueryPlan<T extends Serializable> extends Serializable {
    /*
     Map a query on shards, then reduce into a result.
     */

    // On which keys should the query execute?
    public List<Integer> keysForQuery();
    // Execute the query on an shard (Map).
    public T queryShard(Shard shard);
    // Aggregate the outputs of queries on shards (Reduce).
    public String aggregateShardQueries(List<T> shardQueryResults);
}

package edu.stanford.futuredata.uniserve.interfaces;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

public interface QueryPlan {
    /*
     Map a query on shards, then reduce into a result.
     */

    // On which keys should the query execute?
    public List<Integer> keysForQuery();
    // Execute the query on an shard (Map).
    public ObjectOutputStream queryShard(Shard shard);
    // Aggregate the outputs of queries on shards (Reduce).
    public String aggregateShardQueries(List<ObjectInputStream> shardQueryResults);
}

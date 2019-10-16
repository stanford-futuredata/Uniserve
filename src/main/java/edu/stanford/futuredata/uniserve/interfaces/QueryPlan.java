package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.util.List;

public interface QueryPlan {
    /*
     Map a query on shards, then reduce into a result.
     */

    // On which keys should the query execute?
    public List<Integer> keysForQuery();
    // Execute the query on an shard (Map).
    public ByteString queryShard(Shard shard);
    // Aggregate the outputs of queries on shards (Reduce).
    public String aggregateShardQueries(List<ByteString> shardQueryResults);
}

package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;

public interface ReadQueryPlan<S extends Shard, T> extends Serializable {
    /*
     Execute a read query.
     */

    // Keys on which the query executes.  Query will execute on all shards containing any key from the list.
    // Include -1 to execute on all shards.
    List<Integer> keysForQuery();
    // This function will execute on each shard containing at least one key from keysForQuery.
    ByteString queryShard(S shard);
    // For incrementally updated materialized views, query in a time range.
    ByteString queryShard(S shard, long startTime, long endTime);
    // Combine multiple intermediates into one.
    ByteString combineIntermediates(List<ByteString> intermediates);
    // The query will return the result of this function executed on all results from queryShard.
    T aggregateShardQueries(List<ByteString> shardQueryResults);
    // Return an estimate of the cost of queryShard.  Always called after queryShard.
    int getQueryCost();
    // Get query plans for subqueries.
    List<ReadQueryPlan> getSubQueries();
    // Set results of subqueries.
    void setSubQueryResults(List<Object> subQueryResults);
}

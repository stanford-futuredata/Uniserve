package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface ReadQueryPlan<S extends Shard, I extends Serializable, T> extends Serializable {
    /*
     Execute a read query.
     */

    // Keys on which the query executes.  Query will execute on all shards containing any key from the list.
    // Include -1 to execute on all shards.
    List<Integer> keysForQuery();
    // This function will execute on each shard containing at least one key from keysForQuery.
    I queryShard(S shard);
    // The query will return the result of this function executed on all results from queryShard.
    T aggregateShardQueries(List<I> shardQueryResults);
    // Get query plans for subqueries.
    List<ReadQueryPlan> getSubQueries();
    // Set results of subqueries.
    void setSubQueryResults(List<Object> subQueryResults);
}

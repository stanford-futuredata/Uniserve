package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface ReadQueryPlan<S extends Shard, T> extends Serializable {
    /*
     Execute a read query.
     */

    // Which tables are being queried?
    List<String> getQueriedTables();
    // Keys on which the query executes.  Query will execute on all shards containing any key from the list.
    // Include -1 to execute on all shards.
    Map<String, List<Integer>> keysForQuery();
    // Shuffle needed?  False if data already partitioned on the correct key.
    Map<String, Boolean> shuffleNeeded();
    // This function will execute on each shard containing at least one key from keysForQuery.
    ByteString queryShard(List<S> shard); // TODO:  Have MVs use the standard (shuffle) interface.
    // For incrementally updated materialized views, query all rows whose timestamps satisfy startTime < t <= endTime
    ByteString queryShard(S shard, long startTime, long endTime);
    // Combine multiple intermediates into one.
    ByteString combineIntermediates(List<ByteString> intermediates);
    // Mapper.
    Map<Integer, ByteString> mapper(S shard, String tableName, int numReducers);
    // Reducer.
    ByteString reducer(Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards, Map<String, List<S>> localShards);
    // The query will return the result of this function executed on all results from queryShard.
    T aggregateShardQueries(List<ByteString> shardQueryResults);
    // Get query plans for subqueries.
    List<ReadQueryPlan> getSubQueries();
    // Set results of subqueries.
    void setSubQueryResults(List<Object> subQueryResults);
}

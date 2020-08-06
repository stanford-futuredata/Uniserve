package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface AnchoredReadQueryPlan<S extends Shard, T> extends Serializable {

    // Which tables are being queried?
    List<String> getQueriedTables();
    // Keys on which the query executes.  Query will execute on all shards containing any key from the list.
    // Include -1 to execute on all shards.
    Map<String, List<Integer>> keysForQuery();
    // Which table does not need shuffling?
    String getAnchorTable();
    // This function will execute on each shard containing at least one key from keysForQuery.
    ByteString queryShard(List<S> shard); // TODO:  Have MVs use the standard (shuffle) interface.
    // For incrementally updated materialized views, query all rows whose timestamps satisfy startTime < t <= endTime
    ByteString queryShard(S shard, long startTime, long endTime);
    // Combine multiple intermediates into one.
    ByteString combineIntermediates(List<ByteString> intermediates);
    // Get partition keys.
    List<Integer> getPartitionKeys(Shard s); // TODO:  Maybe this shouldn't be integer.
    // Mapper.
    Map<Integer, ByteString> mapper(S shard, Map<Integer, List<Integer>> partitionKeys);
    // Reducer.
    ByteString reducer(S localShard, Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards);
    // The query will return the result of this function executed on all results from queryShard.
    T aggregateShardQueries(List<ByteString> shardQueryResults);
}

package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface ShuffleReadQueryPlan<S extends Shard, T> extends Serializable {
    // Which tables are being queried?
    List<String> getQueriedTables();
    // Keys on which the query executes.  Query will execute on all shards containing any key from the list.
    // Include -1 to execute on all shards.
    Map<String, List<Integer>> keysForQuery();
    // Mapper.
    Map<Integer, ByteString> mapper(S shard, int numReducers);
    // Reducer.
    ByteString reducer(Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards);
    // The query will return the result of this function executed on all results from queryShard.
    T aggregateShardQueries(List<ByteString> shardQueryResults);
}

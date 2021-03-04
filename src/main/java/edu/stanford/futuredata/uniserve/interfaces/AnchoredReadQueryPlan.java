package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface AnchoredReadQueryPlan<S extends Shard, T> extends Serializable {

    // Which tables are being queried?
    List<String> getQueriedTables();
    // Keys on which the query executes.  Query will execute on all shards containing any key from the list.
    // Include -1 to execute on all shards.
    Map<String, List<Integer>> keysForQuery();
    // Which table does not need shuffling?
    String getAnchorTable();
    // Get partition keys.
    List<Integer> getPartitionKeys(S s);
    // Mapper.
    Map<Integer, List<ByteString>> mapper(S shard, Map<Integer, List<Integer>> partitionKeys);
    // Reducer.
    ByteString reducer(S localShard, Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards);
    // Reducer.
    default void reducer(S localShard, Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards, S returnShard) { };
    // Return an aggregate or shard?  (Default aggregate)
    default Optional<String> returnTableName() {return Optional.empty();}
    // The query will return the result of this function executed on all results from queryShard.
    T aggregateShardQueries(List<ByteString> shardQueryResults);
}

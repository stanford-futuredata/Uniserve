package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KVMaterializedViewSum implements ReadQueryPlan<KVShard, Integer> {

    @Override
    public List<String> getQueriedTables() {
        return Collections.singletonList("table");
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of("table", Collections.singletonList(-1));
    }

    @Override
    public Map<String, Boolean> shuffleNeeded() {
        return Map.of("table", false);
    }

    @Override
    public ByteString queryShard(List<KVShard> shard) {
        return Utilities.objectToByteString(shard.get(0).sumRows(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public ByteString queryShard(KVShard shard, long startTime, long endTime) {
        return Utilities.objectToByteString(shard.sumRows(startTime, endTime));
    }

    @Override
    public ByteString combineIntermediates(List<ByteString> intermediates) {
        return Utilities.objectToByteString(intermediates.stream().mapToInt(i -> (Integer) Utilities.byteStringToObject(i)).sum());
    }

    @Override
    public Map<Integer, ByteString> mapper(KVShard shard, String tableName, int numReducers) {
        return null;
    }

    @Override
    public ByteString reducer(Map<String, List<ByteString>> ephemeralData, List<KVShard> ephemeralShards) {
        return null;
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return shardQueryResults.stream().mapToInt(i -> (Integer) Utilities.byteStringToObject(i)).sum();
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

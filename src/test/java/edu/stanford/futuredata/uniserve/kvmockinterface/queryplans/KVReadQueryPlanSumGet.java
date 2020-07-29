package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KVReadQueryPlanSumGet implements AnchoredReadQueryPlan<KVShard, Integer> {

    private final List<Integer> keys;

    public KVReadQueryPlanSumGet(List<Integer> keys) {
        this.keys = keys;
    }

    @Override
    public List<String> getQueriedTables() {
        return Collections.singletonList("table");
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of("table", keys);
    }

    @Override
    public String getAnchorTable() {
        return "table";
    }

    @Override
    public ByteString queryShard(List<KVShard> shard) {
        int sum = 0;
        for (Integer key : keys) {
            Optional<Integer> value = shard.get(0).queryKey(key);
            if (value.isPresent()) {
                sum += value.get();
            }
        }
        return Utilities.objectToByteString(sum);
    }

    @Override
    public ByteString queryShard(KVShard shard, long startTime, long endTime) {
        return null;
    }

    @Override
    public ByteString combineIntermediates(List<ByteString> intermediates) {
        return null;
    }

    @Override
    public List<Integer> getPartitionKeys(Shard s) {
        return null;
    }

    @Override
    public Map<Integer, ByteString> mapper(KVShard shard, Map<Integer, List<Integer>> partitionKeys) {
        assert(false);
        return null;
    }

    @Override
    public ByteString reducer(KVShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards) {
        int sum = 0;
        for (Integer key : keys) {
            Optional<Integer> value = localShard.queryKey(key);
            if (value.isPresent()) {
                sum += value.get();
            }
        }
        return Utilities.objectToByteString(sum);
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return shardQueryResults.stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
    }

    @Override
    public List<AnchoredReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

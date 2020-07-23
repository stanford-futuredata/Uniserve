package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KVReadQueryPlanSumGet implements ReadQueryPlan<KVShard, Integer> {

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
    public Map<String, Boolean> shuffleNeeded() {
        return Map.of("table", false);
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
    public Map<Integer, ByteString> mapper(KVShard shard, String tableName, int numReducers) {
        assert(false);
        return null;
    }

    @Override
    public ByteString reducer(Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards, Map<String, List<KVShard>> localShards) {
        int sum = 0;
        for (KVShard shard: localShards.get("table")) {
            for (Integer key : keys) {
                Optional<Integer> value = shard.queryKey(key);
                if (value.isPresent()) {
                    sum += value.get();
                }
            }
        }
        return Utilities.objectToByteString(sum);
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return shardQueryResults.stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

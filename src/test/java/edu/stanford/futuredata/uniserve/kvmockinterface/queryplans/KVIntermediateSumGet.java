package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KVIntermediateSumGet implements AnchoredReadQueryPlan<KVShard, Map<String, Map<Integer, Integer>>> {

    private final List<Integer> keys;

    public KVIntermediateSumGet(List<Integer> keys) {
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
    public List<Integer> getPartitionKeys(KVShard s) {
        return null;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(KVShard shard, Map<Integer, List<Integer>> partitionKeys) {
        assert(false);
        return null;
    }

    @Override
    public ByteString gather(KVShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards) {
        return null;
    }

    @Override
    public void gather(KVShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards, KVShard bob) {
        int sum = 0;
        for (Integer key : keys) {
            Optional<Integer> value = localShard.queryKey(key);
            if (value.isPresent()) {
                sum += value.get();
            }
        }
        bob.KVMap.put(0, sum);
    }

    @Override
    public Optional<String> returnTableName() {
        return Optional.of("intermediate1");
    }

    @Override
    public Map<String, Map<Integer, Integer>> aggregateShardQueries(List<ByteString> shardQueryResults) {
        return null;
    }

}

package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.*;

public class KVFilterSumGet implements AnchoredReadQueryPlan<KVShard, Integer> {

    private final List<Integer> keys;

    public KVFilterSumGet(List<Integer> keys) {
        this.keys = keys;
    }

    @Override
    public List<String> getQueriedTables() {
        return List.of("table", "intermediate1");
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
        return List.of();
    }

    @Override
    public List<AnchoredReadQueryPlan<KVShard, Map<String, Map<Integer, Integer>>>> getSubQueries() {
        return List.of(new KVIntermediateSumGet(keys));
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(KVShard shard, Map<Integer, List<Integer>> partitionKeys) {
        int sum = shard.KVMap.getOrDefault(0, 0);
        ByteString b = Utilities.objectToByteString(sum);
        Map<Integer, List<ByteString>> ret = new HashMap<>();
        partitionKeys.keySet().forEach(k -> ret.put(k, List.of(b)));
        return ret;
    }

    @Override
    public ByteString gather(KVShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards) {
        int oldSum = ephemeralData.get("intermediate1").stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
        int average = oldSum / keys.size();
        int sum = 0;
        for (Integer key : keys) {
            Optional<Integer> value = localShard.queryKey(key);
            if (key < average && value.isPresent()) {
                sum += value.get();
            }
        }
        return Utilities.objectToByteString(sum);

    }

    @Override
    public Integer combine(List<ByteString> shardQueryResults) {
        return shardQueryResults.stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
    }
}

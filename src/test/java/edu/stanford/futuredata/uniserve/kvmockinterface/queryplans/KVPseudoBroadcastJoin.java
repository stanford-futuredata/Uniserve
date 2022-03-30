package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVPseudoBroadcastJoin implements AnchoredReadQueryPlan<KVShard, Integer> {

    private final String tableOne;
    private final String tableTwo;

    public KVPseudoBroadcastJoin(String tableOne, String tableTwo) {
        this.tableOne = tableOne;
        this.tableTwo = tableTwo;
    }

    @Override
    public List<String> getQueriedTables() {
        return List.of(tableOne, tableTwo);
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tableOne, Collections.singletonList(-1),
                tableTwo, Collections.singletonList(-1));
    }

    @Override
    public String getAnchorTable() {
        return tableOne;
    }

    @Override
    public List<Integer> getPartitionKeys(KVShard s) {
        return Collections.emptyList();
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(KVShard shard, Map<Integer, List<Integer>> partitionKeys) {
        ByteString b = Utilities.objectToByteString((ConcurrentHashMap<Integer, Integer>) shard.KVMap);
        HashMap<Integer, List<ByteString>> ret = new HashMap<>();
        for (int i: partitionKeys.keySet()) {
            ret.put(i, List.of(b));
        }
        return ret;
    }

    @Override
    public ByteString gather(KVShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards) {
        Map<Integer, Integer> KVMapTwo = (Map<Integer, Integer>) Utilities.byteStringToObject(ephemeralData.get(tableTwo).get(0));
        int sum = 0;
        for (int k : localShard.KVMap.keySet()) {
            if (KVMapTwo.containsKey(k)) {
                sum += localShard.KVMap.get(k) + KVMapTwo.get(k);
            }
        }
        return Utilities.objectToByteString(sum);
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return shardQueryResults.stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
    }

}

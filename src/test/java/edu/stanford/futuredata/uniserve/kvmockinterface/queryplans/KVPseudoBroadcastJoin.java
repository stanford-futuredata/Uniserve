package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class KVPseudoBroadcastJoin implements ReadQueryPlan<KVShard, Integer> {

    private final List<String> tables;

    public KVPseudoBroadcastJoin(String tableOne, String tableTwo) {
        this.tables = List.of(tableOne, tableTwo);
    }

    @Override
    public List<String> getQueriedTables() {
        return tables;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tables.get(0), Collections.singletonList(-1),
                tables.get(1), Collections.singletonList(-1));
    }

    @Override
    public Map<String, Boolean> shuffleNeeded() {
        return Map.of(tables.get(0), false, tables.get(1), true);
    }

    @Override
    public ByteString queryShard(List<KVShard> shard) {
        return null;
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
        ByteString b = Utilities.objectToByteString((ConcurrentHashMap<Integer, Integer>) shard.KVMap);
        HashMap<Integer, ByteString> ret = new HashMap<>();
        for (int i = 0; i < numReducers; i++) {
            ret.put(i, b);
        }
        return ret;
    }

    @Override
    public ByteString reducer(Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards, Map<String, List<KVShard>> localShards) {
        Map<Integer, Integer> KVMapTwo = (Map<Integer, Integer>) Utilities.byteStringToObject(ephemeralData.get(tables.get(1)).get(0));
        int sum = 0;
        for (KVShard tableOneShard: localShards.get(tables.get(0))) {
            for (int k : tableOneShard.KVMap.keySet()) {
                if (KVMapTwo.containsKey(k)) {
                    sum += tableOneShard.KVMap.get(k) + KVMapTwo.get(k);
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

package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KVReadQueryPlanGet implements AnchoredReadQueryPlan<KVShard, Integer> {

    private final String tableName;

    private final Integer key;

    public KVReadQueryPlanGet(Integer key) {
        this.key = key;
        this.tableName = "table";
    }

    public KVReadQueryPlanGet(String tableName, Integer key) {
        this.key = key;
        this.tableName = tableName;
    }

    @Override
    public List<String> getQueriedTables() {
        return Collections.singletonList(tableName);
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tableName, List.of(key));
    }

    @Override
    public String getAnchorTable() {
        return tableName;
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
        return Utilities.objectToByteString(localShard.queryKey(key).get());
    }

    @Override
    public Integer combine(List<ByteString> shardQueryResults) {
        return (Integer) Utilities.byteStringToObject(shardQueryResults.get(0));
    }

}

package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
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
        return Utilities.objectToByteString(localShard.queryKey(key).get());
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return (Integer) Utilities.byteStringToObject(shardQueryResults.get(0));
    }

    @Override
    public List<AnchoredReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

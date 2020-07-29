package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KVReadQueryPlanNested implements AnchoredReadQueryPlan<KVShard, Integer> {
    // Return the value corresponding to the key corresponding to innerKey.

    private final KVReadQueryPlanGet subQuery;
    private Integer innerKeyValue;

    public KVReadQueryPlanNested(Integer innerKey) {
        this.subQuery = new KVReadQueryPlanGet(innerKey);
    }

    @Override
    public List<String> getQueriedTables() {
        return Collections.singletonList("table");
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of("table", List.of(innerKeyValue));
    }

    @Override
    public String getAnchorTable() {
        return "table";
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
        return Utilities.objectToByteString(localShard.queryKey(innerKeyValue).get());
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return (Integer) Utilities.byteStringToObject(shardQueryResults.get(0));
    }

    @Override
    public List<AnchoredReadQueryPlan> getSubQueries() {return Collections.singletonList(subQuery);}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {
        assert(subQueryResults.size() == 1);
        this.innerKeyValue = (Integer) subQueryResults.get(0);
    }
}

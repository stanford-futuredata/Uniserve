package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KVReadQueryPlanNested implements ReadQueryPlan<KVShard, Integer> {
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
    public Map<String, Boolean> shuffleNeeded() {
        return Map.of("table", false);
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
        assert(numReducers == 1);
        return Map.of(0, Utilities.objectToByteString(shard.queryKey(this.innerKeyValue).get()));
    }

    @Override
    public ByteString reducer(Map<String, List<ByteString>> ephemeralData, List<KVShard> ephemeralShards) {
        return ephemeralData.get("table").get(0);
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return (Integer) Utilities.byteStringToObject(shardQueryResults.get(0));
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.singletonList(subQuery);}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {
        assert(subQueryResults.size() == 1);
        this.innerKeyValue = (Integer) subQueryResults.get(0);
    }
}

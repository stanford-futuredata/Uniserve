package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.KVReadQueryPlanGet;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;

public class KVReadQueryPlanNested implements ReadQueryPlan<KVShard, Integer> {
    // Return the value corresponding to the key corresponding to innerKey.

    private final KVReadQueryPlanGet subQuery;
    private Integer innerKeyValue;

    public KVReadQueryPlanNested(Integer innerKey) {
        this.subQuery = new KVReadQueryPlanGet(innerKey);
    }

    @Override
    public String getQueriedTable() {
        return "table";
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(this.innerKeyValue);
    }

    @Override
    public ByteString queryShard(KVShard shard) {
        return Utilities.objectToByteString(shard.queryKey(this.innerKeyValue).get());
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
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return (Integer) Utilities.byteStringToObject(shardQueryResults.get(0));
    }

    @Override
    public int getQueryCost() {
        return 1;
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.singletonList(subQuery);}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {
        assert(subQueryResults.size() == 1);
        this.innerKeyValue = (Integer) subQueryResults.get(0);
    }
}

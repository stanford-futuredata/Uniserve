package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;

import java.util.Collections;
import java.util.List;

public class KVReadQueryPlanNested implements ReadQueryPlan<KVShard, Integer, Integer> {
    // Return the value corresponding to the key corresponding to innerKey.

    private final KVReadQueryPlanGet subQuery;
    private Integer innerKeyValue;

    public KVReadQueryPlanNested(Integer innerKey) {
        this.subQuery = new KVReadQueryPlanGet(innerKey);
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(this.innerKeyValue);
    }

    @Override
    public Integer queryShard(KVShard shard) {
        return shard.queryKey(this.innerKeyValue).get();
    }

    @Override
    public Integer aggregateShardQueries(List<Integer> shardQueryResults) {
        return shardQueryResults.get(0);
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.singletonList(subQuery);}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {
        assert(subQueryResults.size() == 1);
        this.innerKeyValue = (Integer) subQueryResults.get(0);
    }
}

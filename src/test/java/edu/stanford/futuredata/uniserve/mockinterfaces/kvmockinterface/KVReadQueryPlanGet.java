package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;

import java.util.Collections;
import java.util.List;

public class KVReadQueryPlanGet implements ReadQueryPlan<KVShard, Integer, Integer> {

    private final Integer key;

    public KVReadQueryPlanGet(Integer key) {
        this.key = key;
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(this.key);
    }

    @Override
    public Integer queryShard(KVShard shard) {
        return shard.queryKey(this.key).get();
    }

    @Override
    public Integer aggregateShardQueries(List<Integer> shardQueryResults) {
        return shardQueryResults.get(0);
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

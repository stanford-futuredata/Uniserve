package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class KVQueryPlanSumGet implements QueryPlan<KVShard, Integer, Integer> {

    private final List<Integer> keys;

    public KVQueryPlanSumGet(List<Integer> keys) {
        this.keys = keys;
    }

    @Override
    public List<Integer> keysForQuery() {
        return this.keys;
    }

    @Override
    public Integer queryShard(KVShard shard) {
        Integer sum = 0;
        KVShard kvShard = shard;
        for (Integer key : keys) {
            Optional<Integer> value = kvShard.queryKey(key);
            if (value.isPresent()) {
                sum += value.get();
            }
        }
        return sum;
    }

    @Override
    public Integer aggregateShardQueries(List<Integer> shardQueryResults) {
        return shardQueryResults.stream().mapToInt(i -> i).sum();
    }

    @Override
    public List<QueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

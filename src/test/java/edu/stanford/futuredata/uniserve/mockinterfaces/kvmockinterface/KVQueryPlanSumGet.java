package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class KVQueryPlanSumGet implements QueryPlan<Integer> {

    private final List<Integer> keys;

    public KVQueryPlanSumGet(List<Integer> keys) {
        this.keys = keys;
    }

    @Override
    public List<Integer> keysForQuery() {
        return this.keys;
    }

    @Override
    public Integer queryShard(Shard shard) {
        Integer sum = 0;
        KVShard kvShard = (KVShard) shard;
        for (Integer key : keys) {
            Optional<Integer> value = kvShard.queryKey(key);
            if (value.isPresent()) {
                sum += value.get();
            }
        }
        return sum;
    }

    @Override
    public String aggregateShardQueries(List<Integer> shardQueryResults) {
        int sum = shardQueryResults.stream().mapToInt(i -> i).sum();
        return Integer.toString(sum);
    }
}

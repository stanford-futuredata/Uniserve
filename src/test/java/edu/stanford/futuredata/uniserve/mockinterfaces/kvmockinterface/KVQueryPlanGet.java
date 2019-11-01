package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.Collections;
import java.util.List;

public class KVQueryPlanGet implements QueryPlan<KVShard, Integer, Integer> {

    private final Integer key;

    public KVQueryPlanGet(Integer key) {
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
}

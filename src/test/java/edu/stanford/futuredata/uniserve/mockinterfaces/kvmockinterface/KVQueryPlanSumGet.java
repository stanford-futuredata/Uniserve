package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class KVQueryPlanSumGet implements QueryPlan {

    private final List<Integer> keys;

    public KVQueryPlanSumGet(List<Integer> keys) {
        this.keys = keys;
    }

    @Override
    public List<Integer> keysForQuery() {
        return this.keys;
    }

    @Override
    public ByteString queryShard(Shard shard) {
        Integer sum = 0;
        KVShard kvShard = (KVShard) shard;
        for (Integer key : keys) {
            Optional<Integer> value = kvShard.queryKey(key);
            if (value.isPresent()) {
                sum += value.get();
            }
        }
        return ByteString.copyFrom(sum.toString().getBytes());
    }

    @Override
    public String aggregateShardQueries(List<ByteString> shardQueryResults) {
        List<Integer> shardQueryIntegers = shardQueryResults.stream().map(b -> Integer.parseInt(new String(b.toByteArray()))).collect(Collectors.toList());
        int sum = shardQueryIntegers.stream().mapToInt(i -> i).sum();
        return Integer.toString(sum);
    }
}

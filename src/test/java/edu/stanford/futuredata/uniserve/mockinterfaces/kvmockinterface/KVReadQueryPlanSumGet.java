package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class KVReadQueryPlanSumGet implements ReadQueryPlan<KVShard, Integer> {

    private final List<Integer> keys;

    public KVReadQueryPlanSumGet(List<Integer> keys) {
        this.keys = keys;
    }

    @Override
    public List<Integer> keysForQuery() {
        return this.keys;
    }

    @Override
    public List<ByteString> queryShard(KVShard shard) {
        Integer sum = 0;
        KVShard kvShard = shard;
        for (Integer key : keys) {
            Optional<Integer> value = kvShard.queryKey(key);
            if (value.isPresent()) {
                sum += value.get();
            }
        }
        return Collections.singletonList(Utilities.objectToByteString(sum));
    }

    @Override
    public Integer aggregateShardQueries(List<List<ByteString>> shardQueryResults) {
        return shardQueryResults.stream().map(i -> (Integer) Utilities.byteStringToObject(i.get(0))).mapToInt(i -> i).sum();
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

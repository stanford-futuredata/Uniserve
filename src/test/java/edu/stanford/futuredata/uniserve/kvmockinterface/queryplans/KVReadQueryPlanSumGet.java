package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
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
    public List<String> getQueriedTables() {
        return Collections.singletonList("table");
    }

    @Override
    public Optional<List<String>> getShuffleColumns() {
        return Optional.empty();
    }

    @Override
    public List<Integer> keysForQuery() {
        return this.keys;
    }

    @Override
    public ByteString queryShard(List<KVShard> shard) {
        int sum = 0;
        for (Integer key : keys) {
            Optional<Integer> value = shard.get(0).queryKey(key);
            if (value.isPresent()) {
                sum += value.get();
            }
        }
        return Utilities.objectToByteString(sum);
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
        return shardQueryResults.stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
    }

    @Override
    public int getQueryCost() {
        return 1;
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

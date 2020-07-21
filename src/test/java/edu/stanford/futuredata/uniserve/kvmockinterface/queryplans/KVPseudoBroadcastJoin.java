package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class KVPseudoBroadcastJoin implements ReadQueryPlan<KVShard, Integer> {

    private final List<String> tables;

    public KVPseudoBroadcastJoin(String tableOne, String tableTwo) {
        this.tables = List.of(tableOne, tableTwo);
    }

    @Override
    public List<String> getQueriedTables() {
        return tables;
    }

    @Override
    public Optional<List<String>> getShuffleColumns() {
        return Optional.empty();
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(-1);
    }

    @Override
    public ByteString queryShard(List<KVShard> shard) {
        int sum = 0;
        KVShard shardOne = shard.get(0);
        KVShard shardTwo = shard.get(1);
        for (int k : shardOne.KVMap.keySet()) {
            if (shardTwo.KVMap.containsKey(k)) {
                sum += shardOne.KVMap.get(k) + shardTwo.KVMap.get(k);
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

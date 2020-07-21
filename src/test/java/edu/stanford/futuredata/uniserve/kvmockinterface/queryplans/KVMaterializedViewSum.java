package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class KVMaterializedViewSum implements ReadQueryPlan<KVShard, Integer> {

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
        return Collections.singletonList(-1);
    }

    @Override
    public ByteString queryShard(List<KVShard> shard) {
        return Utilities.objectToByteString(shard.get(0).sumRows(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public ByteString queryShard(KVShard shard, long startTime, long endTime) {
        return Utilities.objectToByteString(shard.sumRows(startTime, endTime));
    }

    @Override
    public ByteString combineIntermediates(List<ByteString> intermediates) {
        return Utilities.objectToByteString(intermediates.stream().mapToInt(i -> (Integer) Utilities.byteStringToObject(i)).sum());
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return shardQueryResults.stream().mapToInt(i -> (Integer) Utilities.byteStringToObject(i)).sum();
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

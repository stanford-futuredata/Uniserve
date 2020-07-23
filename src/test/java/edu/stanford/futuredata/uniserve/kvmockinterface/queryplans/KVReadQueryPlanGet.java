package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KVReadQueryPlanGet implements ReadQueryPlan<KVShard, Integer> {

    private final String tableName;

    private final Integer key;

    public KVReadQueryPlanGet(Integer key) {
        this.key = key;
        this.tableName = "table";
    }

    public KVReadQueryPlanGet(String tableName, Integer key) {
        this.key = key;
        this.tableName = tableName;
    }

    @Override
    public List<String> getQueriedTables() {
        return Collections.singletonList(tableName);
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tableName, List.of(key));
    }

    @Override
    public Map<String, Boolean> shuffleNeeded() {
        return Map.of(tableName, false);
    }

    @Override
    public ByteString queryShard(List<KVShard> shard) {
        return null;
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
    public Map<Integer, ByteString> mapper(KVShard shard, String tableName, int numReducers) {
        assert(numReducers == 1);
        return Map.of(0, Utilities.objectToByteString(shard.queryKey(this.key).get()));
    }

    @Override
    public ByteString reducer(Map<String, List<ByteString>> ephemeralData, List<KVShard> ephemeralShards) {
        List<ByteString> tableEphemeralData = ephemeralData.get(tableName);
        if (tableEphemeralData.size() > 0) {
            assert(tableEphemeralData.size() == 1);
            return tableEphemeralData.get(0);
        } else {
            return ByteString.EMPTY;
        }
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        for (ByteString b: shardQueryResults) {
            if(!b.isEmpty()) {
                return (Integer) Utilities.byteStringToObject(b);
            }
        }
        return null;
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

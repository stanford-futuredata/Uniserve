package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;

public class KVReadQueryPlanGet implements ReadQueryPlan<KVShard, Integer> {

    private final Integer key;

    public KVReadQueryPlanGet(Integer key) {
        this.key = key;
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(this.key);
    }

    @Override
    public List<ByteString> queryShard(KVShard shard) {
        return Collections.singletonList(Utilities.objectToByteString(shard.queryKey(this.key).get()));
    }

    @Override
    public Integer aggregateShardQueries(List<List<ByteString>> shardQueryResults) {
        return (Integer) Utilities.byteStringToObject(shardQueryResults.get(0).get(0));
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

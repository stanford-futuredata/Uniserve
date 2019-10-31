package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.Collections;
import java.util.List;

public class KVQueryPlanGet implements QueryPlan {

    private final Integer key;

    public KVQueryPlanGet(Integer key) {
        this.key = key;
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(this.key);
    }

    @Override
    public ByteString queryShard(Shard shard) {
        Integer value = ((KVShard) shard).queryKey(this.key).get();
        return ByteString.copyFrom(value.toString().getBytes());
    }

    @Override
    public String aggregateShardQueries(List<ByteString> shardQueryResults) {
        ByteString keyResult = shardQueryResults.get(0);
        return new String(keyResult.toByteArray());
    }
}

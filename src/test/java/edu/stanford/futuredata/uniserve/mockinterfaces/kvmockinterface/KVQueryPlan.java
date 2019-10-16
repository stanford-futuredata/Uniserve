package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.Arrays;
import java.util.List;

public class KVQueryPlan implements QueryPlan {

    @Override
    public List<Integer> keysForQuery() {
        return Arrays.asList(0, 1);
    }

    @Override
    public ByteString queryShard(Shard shard) {
        String testString = "bob";
        return ByteString.copyFrom(testString.getBytes());
    }

    @Override
    public String aggregateShardQueries(List<ByteString> shardQueryResults) {
        return "foo";
    }
}

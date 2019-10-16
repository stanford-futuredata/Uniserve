package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.Arrays;
import java.util.List;

public class KVShard implements Shard {
    @Override
    public int addRow(ByteString row) {
        return 0;
    }
    @Override
    public List<String> shardToData() {
        return Arrays.asList("foo", "bar");
    }
    @Override
    public int shardFromData(List<String> data) {
        return 0;
    }
}

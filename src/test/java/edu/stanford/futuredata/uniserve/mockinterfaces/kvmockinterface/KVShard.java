package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.*;

public class KVShard implements Shard {

    private final Map<Integer, Integer> KVMap;

    public KVShard() {
        this.KVMap = new HashMap<>();
    }

    @Override
    public int addRow(Row row) {
        KVRow kvRow = (KVRow) row;
        KVMap.put(kvRow.getKey(), kvRow.getValue());
        return 0;
    }

    public Optional<Integer> queryKey(Integer key) {

        if (KVMap.containsKey(key)) {
            return Optional.of(KVMap.get(key));
        } else {
            return Optional.empty();
        }
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

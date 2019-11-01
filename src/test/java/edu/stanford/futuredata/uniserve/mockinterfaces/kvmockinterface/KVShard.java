package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.*;

public class KVShard implements Shard<KVRow> {

    private final Map<Integer, Integer> KVMap;

    public KVShard() {
        this.KVMap = new HashMap<>();
    }

    @Override
    public int addRow(KVRow row) {
        KVMap.put(row.getKey(), row.getValue());
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

package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class KVShard implements Shard<KVRow> {

    private final Map<Integer, Integer> KVMap;
    private final static AtomicInteger numShards = new AtomicInteger(0);
    private final Integer shardNum;

    public KVShard() {
        this.shardNum = numShards.getAndIncrement();
        this.KVMap = new ConcurrentHashMap<>();
    }

    @Override
    public int addRow(KVRow row) {
        KVMap.put(row.getKey(), row.getValue());
        return 0;
    }

    @Override
    public void destroy() {}

    public Optional<Integer> queryKey(Integer key) {
        if (KVMap.containsKey(key)) {
            return Optional.of(KVMap.get(key));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> shardToData() {
        String shardDir = String.format("/var/tmp/KVShard%d/", shardNum);
        File shardDirFile = new File(shardDir);
        if (!shardDirFile.exists()) {
            shardDirFile.mkdir();
        }
        String mapFile = shardDir + "map.obj";
        try {
            FileOutputStream f = new FileOutputStream(new File(mapFile));
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(KVMap);
            o.close();
            f.close();
        } catch (IOException e) {
            return Optional.empty();
        }
        return Optional.of(shardDir);
    }
}

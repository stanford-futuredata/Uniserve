package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class KVShard implements Shard<KVRow> {

    private final Map<Integer, Integer> KVMap;
    private final static AtomicInteger numShards = new AtomicInteger(0);
    private final Integer shardNum;
    private final Path shardPath;

    public KVShard(Path shardPath) {
        this.shardNum = numShards.getAndIncrement();
        this.KVMap = new ConcurrentHashMap<>();
        this.shardPath = shardPath;
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
    public Optional<Path> shardToData() {
        Path shardDir = Path.of(shardPath.toString(), Integer.toString(shardNum));
        File shardDirFile = shardDir.toFile();
        if (!shardDirFile.exists()) {
            shardDirFile.mkdirs();
        }
        Path mapFile = Path.of(shardDir.toString(), "map.obj");
        try {
            FileOutputStream f = new FileOutputStream(mapFile.toFile());
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

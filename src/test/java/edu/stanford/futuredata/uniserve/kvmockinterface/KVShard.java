package edu.stanford.futuredata.uniserve.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class KVShard implements Shard {
    private static final Logger logger = LoggerFactory.getLogger(KVShard.class);

    public final Map<Integer, Integer> KVMap;
    private final Map<Integer, Long> timestampMap;
    private final static AtomicInteger numShards = new AtomicInteger(0);
    private final Path shardPath;

    List<KVRow> rows;

    public KVShard(Path shardPath, boolean shardExists) throws IOException, ClassNotFoundException {
        if (shardExists) {
            Path mapFile = Path.of(shardPath.toString(), "map.obj");
            FileInputStream f = new FileInputStream(mapFile.toFile());
            ObjectInputStream o = new ObjectInputStream(f);
            this.KVMap = (ConcurrentHashMap<Integer, Integer>) o.readObject();
            o.close();
            f.close();
            Path tsFile = Path.of(shardPath.toString(), "ts.obj");
            f = new FileInputStream(tsFile.toFile());
            o = new ObjectInputStream(f);
            this.timestampMap = (ConcurrentHashMap<Integer, Long>) o.readObject();
            o.close();
            f.close();
        } else {
            this.KVMap = new ConcurrentHashMap<>();
            this.timestampMap = new ConcurrentHashMap<>();
        }
        this.shardPath = shardPath;
    }

    @Override
    public void destroy() {}

    @Override
    public int getMemoryUsage() {
        return KVMap.size();  // TODO:  Return the actual size in memory.
    }

    public Optional<Integer> queryKey(Integer key) {
        if (KVMap.containsKey(key)) {
            return Optional.of(KVMap.get(key));
        } else {
            return Optional.empty();
        }
    }

    public Integer sumRows(long startStamp, long endStamp) {
        int sum = 0;
        for (Integer k: timestampMap.keySet()) {
            long ts = timestampMap.get(k);
            if (ts > startStamp && ts <= endStamp) {
                sum += KVMap.get(k);
            }
        }
        return sum;
    }

    public void setRows(List<KVRow> rows) {
        this.rows = rows;
    }

    public void insertRows() {
        for (KVRow row: rows) {
            KVMap.put(row.getKey(), row.getValue());
            timestampMap.put(row.getKey(), row.getTimeStamp());
        }
    }

    @Override
    public Optional<Path> shardToData() {
        Path mapFile = Path.of(shardPath.toString(), "map.obj");
        Path tsFile = Path.of(shardPath.toString(), "ts.obj");
        try {
            FileOutputStream f = new FileOutputStream(mapFile.toFile());
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(KVMap);
            o.close();
            f.close();
            f = new FileOutputStream(tsFile.toFile());
            o = new ObjectOutputStream(f);
            o.writeObject(timestampMap);
            o.close();
            f.close();
        } catch (IOException e) {
            return Optional.empty();
        }
        return Optional.of(shardPath);
    }

}

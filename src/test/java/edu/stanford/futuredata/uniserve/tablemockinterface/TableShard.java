package edu.stanford.futuredata.uniserve.tablemockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TableShard implements Shard {
    private static final Logger logger = LoggerFactory.getLogger(TableShard.class);

    public final List<Map<String, Integer>> table;
    private final Path shardPath;

    public TableShard(Path shardPath, boolean shardExists) throws IOException, ClassNotFoundException {
        if (shardExists) {
            Path mapFile = Path.of(shardPath.toString(), "map.obj");
            FileInputStream f = new FileInputStream(mapFile.toFile());
            ObjectInputStream o = new ObjectInputStream(f);
            this.table = (ArrayList<Map<String, Integer>>) o.readObject();
            o.close();
            f.close();
        } else {
            this.table = new ArrayList<>();
        }
        this.shardPath = shardPath;
    }

    @Override
    public void destroy() {}

    @Override
    public int getMemoryUsage() {
        return table.size();
    }

    @Override
    public Optional<Path> shardToData() {
        Path mapFile = Path.of(shardPath.toString(), "map.obj");
        try {
            FileOutputStream f = new FileOutputStream(mapFile.toFile());
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(table);
            o.close();
            f.close();
        } catch (IOException e) {
            return Optional.empty();
        }
        return Optional.of(shardPath);
    }
}

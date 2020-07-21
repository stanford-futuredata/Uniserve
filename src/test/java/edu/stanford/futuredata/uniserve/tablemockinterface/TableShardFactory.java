package edu.stanford.futuredata.uniserve.tablemockinterface;

import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class TableShardFactory implements ShardFactory<TableShard> {

    private static final Logger logger = LoggerFactory.getLogger(TableShardFactory.class);

    @Override
    public Optional<TableShard> createNewShard(Path shardPath, int shardNum) {
        try {
            return Optional.of(new TableShard(shardPath, false));
        } catch (IOException | ClassNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<TableShard> createShardFromDir(Path shardPath, int shardNum) {
        try {
            return Optional.of(new TableShard(shardPath, true));
        } catch (IOException | ClassNotFoundException e) {
            logger.warn("Shard creation from directory failed: {}: {}", shardPath.toString(), e.getMessage());
            return Optional.empty();
        }
    }
}

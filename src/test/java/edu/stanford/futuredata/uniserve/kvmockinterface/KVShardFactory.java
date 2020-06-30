package edu.stanford.futuredata.uniserve.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class KVShardFactory implements ShardFactory<KVShard> {

    private static final Logger logger = LoggerFactory.getLogger(KVShardFactory.class);

    @Override
    public Optional<KVShard> createNewShard(Path shardPath, int shardNum) {
        try {
            return Optional.of(new KVShard(shardPath, false));
        } catch (IOException | ClassNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<KVShard> createShardFromDir(Path shardPath, int shardNum) {
        try {
            return Optional.of(new KVShard(shardPath, true));
        } catch (IOException | ClassNotFoundException e) {
            logger.warn("Shard creation from directory failed: {}: {}", shardPath.toString(), e.getMessage());
            return Optional.empty();
        }
    }
}

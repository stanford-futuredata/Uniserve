package edu.stanford.futuredata.uniserve.interfaces;

import java.nio.file.Path;
import java.util.Optional;

public interface ShardFactory<S extends Shard> {
    /*
     Create a shard storing data in a directory.
     */

    // Create a new shard at shardPath.
    Optional<S> createNewShard(Path shardPath, int shardNum);

    // Load a shard serialized at shardPath by shard.shardToData.
    Optional<S> createShardFromDir(Path shardPath, int shardNum);
}

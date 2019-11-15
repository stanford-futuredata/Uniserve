package edu.stanford.futuredata.uniserve.interfaces;

import java.nio.file.Path;
import java.util.Optional;

public interface ShardFactory<S extends Shard> {
    /*
     Create a shard storing data in a directory.
     */

    Optional<S> createShard(Path shardPath);
}

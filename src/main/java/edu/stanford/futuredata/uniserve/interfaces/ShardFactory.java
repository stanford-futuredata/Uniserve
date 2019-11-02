package edu.stanford.futuredata.uniserve.interfaces;

import java.util.Optional;

public interface ShardFactory<S extends Shard> {
    /*
     Creates a shard of data from some parameters.
     */

    Optional<S> createShard();
}

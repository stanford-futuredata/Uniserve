package edu.stanford.futuredata.uniserve.interfaces;

public interface ShardFactory<S extends Shard> {
    /*
     Creates a shard of data from some parameters.
     */

    S createShard();
}

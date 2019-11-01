package edu.stanford.futuredata.uniserve.interfaces;

public interface ShardFactory {
    /*
     Creates a shard of data from some parameters.
     */

    Shard createShard();
}

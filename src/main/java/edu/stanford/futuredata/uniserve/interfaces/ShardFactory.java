package edu.stanford.futuredata.uniserve.interfaces;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

public interface ShardFactory {
    /*
     Creates a shard of data from some parameters.
     */

    public Shard createShard();
}

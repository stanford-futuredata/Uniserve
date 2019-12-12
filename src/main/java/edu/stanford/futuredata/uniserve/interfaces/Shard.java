package edu.stanford.futuredata.uniserve.interfaces;

import java.nio.file.Path;
import java.util.Optional;

public interface Shard {
    /*
     A stateful data structure.

     Shard concurrency contract:
     Writes always have exclusive access to the shard.
     Reads (including shardToData) run concurrently with other reads, but never with writes.
     */

    // Destroy shard data and processes.  After destruction, shard is no longer usable.
    void destroy();
    // Return directory containing shard files.
    Optional<Path> shardToData();
}
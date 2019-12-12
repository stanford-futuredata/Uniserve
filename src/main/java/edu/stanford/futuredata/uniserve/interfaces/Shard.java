package edu.stanford.futuredata.uniserve.interfaces;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public interface Shard<R extends Row> {
    /*
     A stateful data structure.

     Shard concurrency contract:
     Writes (addRow) always have exclusive access to the shard.
     Reads (shardToData and queries) run concurrently with other reads, but never with writes.
     */

    // Add a new row to the shard.
    int insertRows(List<R> rows);
    // Destroy shard data and processes.  After destruction, shard is no longer usable.
    void destroy();
    // Return directory containing shard files.
    Optional<Path> shardToData();
}
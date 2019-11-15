package edu.stanford.futuredata.uniserve.interfaces;

import java.nio.file.Path;
import java.util.Optional;

public interface Shard<R extends Row> {
    /*
     Stores a shard of data.  Probably contains an internal index structure for fast queries.
     For durability, can be saved as files to persistent storage (S3?) and reconstructed.
     */

    // Add a new row to the shard.
    int addRow(R row);
    // Destroy shard data and processes.  After destruction, shard is no longer usable.
    void destroy();
    // Return directory containing shard files.
    Optional<Path> shardToData();
}
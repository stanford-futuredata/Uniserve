package edu.stanford.futuredata.uniserve.interfaces;

import java.util.List;

public interface Shard {
    /*
     Stores a shard of data.  Probably contains an internal index structure for fast queries.
     For durability, can be saved as files to persistent storage (S3?) and reconstructed.
     */

    // Add a new row to the shard.
    int addRow(Row row);
    // Create files from which shard can be reconstructed.
    List<String> shardToData();
    // Reconstruct shard from files.
    int shardFromData(List<String> data);
}
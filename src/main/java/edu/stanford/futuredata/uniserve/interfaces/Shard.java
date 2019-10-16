package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.util.List;

public interface Shard {
    /*
     Stores a shard of data.  Probably contains an internal index structure for fast queries.
     For durability, can be saved as files to persistent storage (S3?) and reconstructed.
     */

    // Add a new row to the shard.
    public int addRow(ByteString row);
    // Create files from which shard can be reconstructed.
    public List<String> shardToData();
    // Reconstruct shard from files.
    public int shardFromData(List<String> data);
}
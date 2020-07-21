package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.nio.file.Path;
import java.util.Optional;

public interface Shard {
    /*
     A stateful data structure.

     Shard concurrency contract:
     Writes never run simultaneously.
     shardToData never runs at the same time as a write.
     Reads run at any time.
     */

    // Return the amount of memory this shard uses in kilobytes.
    int getMemoryUsage();
    // Destroy shard data and processes.  After destruction, shard is no longer usable.
    void destroy();
    // Return a directory containing a serialization of this shard.
    Optional<Path> shardToData();
    // Return an importable blob containing all rows whose values in the specified column hash to the specified
    // value given the specified number of buckets.
    ByteString bulkExport(String columnName, int hashValue, int numBuckets);
    // Import a blob returned by bulkExport.
    boolean bulkImport(ByteString rows);
}
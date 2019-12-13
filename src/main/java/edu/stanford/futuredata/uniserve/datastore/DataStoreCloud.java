package edu.stanford.futuredata.uniserve.datastore;

import java.nio.file.Path;
import java.util.Optional;

public interface DataStoreCloud {
    // Upload a shard directory, return information sufficient to download it.
    public Optional<String> uploadShardToCloud(Path shardDirectory, String shardName, int versionNumber);
    // Download a directory previously uploaded.
    public int downloadShardFromCloud(Path shardDirectory, String shardCloudName);
}

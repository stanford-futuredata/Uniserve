package edu.stanford.futuredata.uniserve.datastore;

import java.util.Optional;

public interface DataStoreCloud {
    // Upload a shard directory, return information sufficient to download it.
    public Optional<String> uploadShardToCloud(String shardDirectory, String shardName);
    // Download a directory previously uploaded.
    public int downloadShardFromCloud(String shardDirectory, String shardCloudName);
}

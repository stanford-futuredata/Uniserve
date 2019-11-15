package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;

import java.nio.file.Path;
import java.util.Optional;

public class KVShardFactory implements ShardFactory<KVShard> {

    @Override
    public Optional<KVShard> createShard(Path shardPath) {
        return Optional.of(new KVShard(shardPath));
    }
}

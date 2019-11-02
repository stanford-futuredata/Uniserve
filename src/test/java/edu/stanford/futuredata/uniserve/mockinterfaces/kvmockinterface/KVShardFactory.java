package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;

import java.util.Optional;

public class KVShardFactory implements ShardFactory<KVShard> {

    @Override
    public Optional<KVShard> createShard() {
        return Optional.of(new KVShard());
    }
}

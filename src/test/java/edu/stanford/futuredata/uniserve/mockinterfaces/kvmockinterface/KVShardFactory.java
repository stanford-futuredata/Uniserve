package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;

public class KVShardFactory implements ShardFactory {

    @Override
    public Shard createShard() {
        return new KVShard();
    }
}

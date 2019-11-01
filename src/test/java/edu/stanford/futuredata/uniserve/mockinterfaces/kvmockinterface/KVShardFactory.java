package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;

public class KVShardFactory implements ShardFactory<KVShard> {

    @Override
    public KVShard createShard() {
        return new KVShard();
    }
}

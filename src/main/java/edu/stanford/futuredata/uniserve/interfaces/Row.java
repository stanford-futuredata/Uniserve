package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;

public interface Row extends Serializable {
    /*
     A row of data.  Exposes a (non-negative) partition key.
     We guarantee that objects with the same key are stored in the same shard.
     */
    int getPartitionKey();
}

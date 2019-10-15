package edu.stanford.futuredata.uniserve.interfaces;

public interface Row {
    /*
     Single row of data.  An implementation of this interface is, roughly, a table schema.
     */

    // Get the row's partition key.  This can be any value (not necessarily unique).  We try to put
    // rows with similar partition keys in the same shard.
    public int getPartitionKey();
}
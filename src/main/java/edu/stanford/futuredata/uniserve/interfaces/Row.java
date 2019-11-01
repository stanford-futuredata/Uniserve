package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;

public interface Row extends Serializable {
    /*
     A row of data.  Exposes a partition key.
     */
    int getPartitionKey();
}

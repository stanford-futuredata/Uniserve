package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface SimpleWriteQueryPlan<R extends Row, S extends Shard> extends Serializable {
    /*
     Execute a write query.
     */

    // What table is being queried?
    String getQueriedTable();
    // Stage a write query on the rows.  Return true if ready to commit, false if must abort.
    boolean write(S shard, List<R> rows);
}

package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface WriteQueryPlan<R extends Row, S extends Shard> extends Serializable {
    /*
     Execute a write query.
     */

    // Stage a write query on the rows.  Return true if ready to commit, false if must abort.
    boolean preCommit(S shard, List<R> rows);
    // Commit the query.
    void commit(S shard);
    // Abort the query.
    void abort(S shard);
}

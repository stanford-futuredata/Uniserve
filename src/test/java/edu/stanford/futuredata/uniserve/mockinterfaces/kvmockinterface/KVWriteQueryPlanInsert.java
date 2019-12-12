package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;

import java.util.List;

public class KVWriteQueryPlanInsert implements WriteQueryPlan<KVRow, KVShard> {

    private List<KVRow> rows;

    @Override
    public boolean preCommit(KVShard shard, List<KVRow> rows) {
        this.rows = rows;
        return true;
    }

    @Override
    public void commit(KVShard shard) {
        shard.insertRows(rows);
    }

    @Override
    public void abort(KVShard shard) {

    }
}

package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;

import java.util.List;

public class KVWriteQueryPlanInsertSlow implements WriteQueryPlan<KVRow, KVShard> {

    @Override
    public boolean preCommit(KVShard shard, List<KVRow> rows) {
        shard.setRows(rows);
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) { }
        return true;
    }

    @Override
    public void commit(KVShard shard) {
        shard.insertRows();
    }

    @Override
    public void abort(KVShard shard) {

    }
}

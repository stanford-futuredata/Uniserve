package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;

import java.util.List;

public class KVWriteQueryPlanInsert implements WriteQueryPlan<KVRow, KVShard> {

    @Override
    public boolean preCommit(KVShard shard, List<KVRow> rows) {
        shard.insertRows(rows);
        return true;
    }

    @Override
    public void commit(KVShard shard) {

    }

    @Override
    public void abort(KVShard shard) {

    }
}

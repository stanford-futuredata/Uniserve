package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;

import java.util.List;

public class KVWriteQueryPlanInsert implements WriteQueryPlan<KVRow, KVShard> {

    @Override
    public String getQueriedTable() {
        return "table";
    }

    @Override
    public boolean preCommit(KVShard shard, List<KVRow> rows) {
        for (KVRow row: rows) {
            if (row.getKey() == 123123123) {
                return false;
            }
        }
        shard.setRows(rows);
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

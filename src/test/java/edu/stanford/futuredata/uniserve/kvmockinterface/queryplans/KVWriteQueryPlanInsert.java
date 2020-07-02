package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;

import java.util.List;

public class KVWriteQueryPlanInsert implements WriteQueryPlan<KVRow, KVShard> {

    private final String tableName;

    public KVWriteQueryPlanInsert() {
        this.tableName = "table";
    }

    public KVWriteQueryPlanInsert(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getQueriedTable() {
        return tableName;
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

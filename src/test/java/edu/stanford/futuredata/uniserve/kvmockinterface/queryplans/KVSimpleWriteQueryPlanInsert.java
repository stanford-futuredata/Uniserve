package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;

import java.util.List;

public class KVSimpleWriteQueryPlanInsert implements SimpleWriteQueryPlan<KVRow, KVShard> {

    private final String tableName;

    public KVSimpleWriteQueryPlanInsert() {
        this.tableName = "table";
    }

    public KVSimpleWriteQueryPlanInsert(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getQueriedTable() {
        return tableName;
    }

    @Override
    public boolean write(KVShard shard, List<KVRow> rows) {
        for (KVRow row : rows) {
            if (row.getKey() == 123123123) {
                return false;
            }
        }
        shard.setRows(rows);
        shard.insertRows();
        return true;
    }
}

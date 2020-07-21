package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableRow;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;

import java.util.List;
import java.util.stream.Collectors;

public class TableWriteInsert implements WriteQueryPlan<TableRow, TableShard> {

    private final String tableName;

    public TableWriteInsert(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getQueriedTable() {
        return tableName;
    }

    @Override
    public boolean preCommit(TableShard shard, List<TableRow> rows) {
        shard.table.addAll(rows.stream().map(TableRow::getRow).collect(Collectors.toList()));
        return true;
    }

    @Override
    public void commit(TableShard shard) {

    }

    @Override
    public void abort(TableShard shard) {

    }
}

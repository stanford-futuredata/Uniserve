package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;

public class TableReadQueryPlanJoin implements ReadQueryPlan<TableShard, Integer> {

    private final List<String> tables;

    public TableReadQueryPlanJoin(String tableOne, String tableTwo) {
        this.tables = List.of(tableOne, tableTwo);
    }

    @Override
    public List<String> getQueriedTables() {
        return tables;
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(-1);
    }

    @Override
    public ByteString queryShard(List<TableShard> shard) {
        int sum = 0;
        return Utilities.objectToByteString(sum);
    }

    @Override
    public ByteString queryShard(TableShard shard, long startTime, long endTime) {
        return null;
    }

    @Override
    public ByteString combineIntermediates(List<ByteString> intermediates) {
        return null;
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return shardQueryResults.stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
    }

    @Override
    public int getQueryCost() {
        return 1;
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}

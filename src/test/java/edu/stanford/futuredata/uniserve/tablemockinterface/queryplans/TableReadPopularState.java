package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TableReadPopularState implements ReadQueryPlan<TableShard, Integer> {

    private static final Logger logger = LoggerFactory.getLogger(TableReadPopularState.class);

    private final List<String> tables;

    public TableReadPopularState(String tableOne, String tableTwo) {
        this.tables = List.of(tableOne, tableTwo);
    }

    @Override
    public List<String> getQueriedTables() {
        return tables;
    }

    @Override
    public Optional<List<String>> getShuffleColumns() {
        return Optional.of(List.of("city", "city"));
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(-1);
    }

    @Override
    public ByteString queryShard(List<TableShard> shards) {
        Map<Integer, Integer> cityToState = new HashMap<>();
        TableShard shardOne = shards.get(0);
        TableShard shardTwo = shards.get(1);
        for (Map<String, Integer> row: shardTwo.table) {
            cityToState.put(row.get("city"), row.get("state"));
        }
        HashMap<Integer, Integer> stateFrequency = new HashMap<>();
        for (Map<String, Integer> row: shardOne.table) {
            int state = cityToState.get(row.get("city"));
            stateFrequency.merge(state, 1, Integer::sum);
        }
        return Utilities.objectToByteString(stateFrequency);
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
        Map<Integer, Integer> stateFrequency = new HashMap<>();
        for (ByteString b: shardQueryResults) {
            Map<Integer, Integer> shardStateFrequency = (HashMap<Integer, Integer>) Utilities.byteStringToObject(b);
            shardStateFrequency.forEach((k, v) -> stateFrequency.merge(k, v, Integer::sum));
        }
        return stateFrequency.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1).get().getKey();
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

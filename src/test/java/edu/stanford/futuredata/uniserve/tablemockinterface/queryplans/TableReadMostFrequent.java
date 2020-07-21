package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.util.*;

// Returns the most frequent value in column "v".
public class TableReadMostFrequent implements ReadQueryPlan<TableShard, Integer> {

    private final List<String> tables;

    public TableReadMostFrequent(String table) {
        this.tables = List.of(table);
    }

    @Override
    public List<String> getQueriedTables() {
        return tables;
    }

    @Override
    public Optional<List<String>> getShuffleColumns() {
        return Optional.of(List.of("v"));
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(-1);
    }

    @Override
    public ByteString queryShard(List<TableShard> shards) {
        Map<Integer, Integer> frequencies = new HashMap<>();
        TableShard s = shards.get(0);
        for (Map<String, Integer> row: s.table) {
            Integer val = row.get("v");
            frequencies.merge(val, 1, Integer::sum);
        }
        Optional<Map.Entry<Integer, Integer>> maxEntry =
                frequencies.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1);
        if (maxEntry.isPresent()) {
            Pair<Integer, Integer> kf = new Pair<>(maxEntry.get().getKey(), maxEntry.get().getValue());
            return Utilities.objectToByteString(kf);
        } else {
            return ByteString.EMPTY;
        }
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
        Integer maxKey = null;
        int maxFrequency = Integer.MIN_VALUE;
        for (ByteString b: shardQueryResults) {
            if (!b.isEmpty()) {
                Pair<Integer, Integer> kf = (Pair<Integer, Integer>) Utilities.byteStringToObject(b);
                int key = kf.getValue0();
                int frequency = kf.getValue1();
                if (frequency > maxFrequency) {
                    maxFrequency = frequency;
                    maxKey = key;
                }
            }
        }
        return maxKey;
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

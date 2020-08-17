package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleReadQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.util.*;

// Returns the most frequent value in column "v".
public class TableReadMostFrequent implements ShuffleReadQueryPlan<TableShard, Integer> {

    private final List<String> tables;

    public TableReadMostFrequent(String table) {
        this.tables = List.of(table);
    }

    @Override
    public List<String> getQueriedTables() {
        return tables;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tables.get(0), Collections.singletonList(-1));
    }

    @Override
    public Map<Integer, List<ByteString>> mapper(TableShard shard, int numReducers) {
        Map<Integer, ArrayList<Map<String, Integer>>> partitionedTables = new HashMap<>();
        for (Map<String, Integer> row: shard.table) {
            int partitionKey = ConsistentHash.hashFunction(row.get("v")) % numReducers;
            partitionedTables.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row);
        }
        HashMap<Integer, List<ByteString>> serializedTables = new HashMap<>();
        partitionedTables.forEach((k, v) -> serializedTables.put(k, List.of(Utilities.objectToByteString(v))));
        for (int i = 0; i < numReducers; i++) {
            if(!serializedTables.containsKey(i)) {
                serializedTables.put(i, List.of(ByteString.EMPTY));
            }
        }
        return serializedTables;
    }

    @Override
    public ByteString reducer(Map<String, List<ByteString>> ephemeralData, Map<String, TableShard> ephemeralShards) {
        Map<Integer, Integer> frequencies = new HashMap<>();
        for (ByteString b: ephemeralData.get(tables.get(0))) {
            if (!b.isEmpty()) {
                List<Map<String, Integer>> table = (List<Map<String, Integer>>) Utilities.byteStringToObject(b);
                for (Map<String, Integer> row : table) {
                    Integer val = row.get("v");
                    frequencies.merge(val, 1, Integer::sum);
                }
            }
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
}

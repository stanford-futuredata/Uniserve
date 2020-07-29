package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.util.*;

// Returns the most frequent value in column "v".
//public class TableReadMostFrequent implements AnchoredReadQueryPlan<TableShard, Integer> {
//
//    private final List<String> tables;
//
//    public TableReadMostFrequent(String table) {
//        this.tables = List.of(table);
//    }
//
//    @Override
//    public List<String> getQueriedTables() {
//        return tables;
//    }
//
//    @Override
//    public Map<String, List<Integer>> keysForQuery() {
//        return Map.of(tables.get(0), Collections.singletonList(-1));
//    }
//
//    @Override
//    public String getAnchorTable() {
//        return Map.of(tables.get(0), true);
//    }
//
//    @Override
//    public ByteString queryShard(List<TableShard> shards) {
//        return ByteString.EMPTY;
//    }
//
//    @Override
//    public ByteString queryShard(TableShard shard, long startTime, long endTime) {
//        return null;
//    }
//
//    @Override
//    public ByteString combineIntermediates(List<ByteString> intermediates) {
//        return null;
//    }
//
//    @Override
//    public Map<Integer, ByteString> mapper(TableShard shard, String tableName, int numReducers) {
//        Map<Integer, ArrayList<Map<String, Integer>>> partitionedTables = new HashMap<>();
//        for (Map<String, Integer> row: shard.table) {
//            int partitionKey = ConsistentHash.hashFunction(row.get("v")) % numReducers;
//            partitionedTables.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row);
//        }
//        HashMap<Integer, ByteString> serializedTables = new HashMap<>();
//        partitionedTables.forEach((k, v) -> serializedTables.put(k, Utilities.objectToByteString(v)));
//        for (int i = 0; i < numReducers; i++) {
//            if(!serializedTables.containsKey(i)) {
//                serializedTables.put(i, ByteString.EMPTY);
//            }
//        }
//        return serializedTables;
//    }
//
//    @Override
//    public ByteString reducer(TableShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, TableShard> ephemeralShards) {
//        Map<Integer, Integer> frequencies = new HashMap<>();
//        for (ByteString b: ephemeralData.get(tables.get(0))) {
//            if (!b.isEmpty()) {
//                List<Map<String, Integer>> table = (List<Map<String, Integer>>) Utilities.byteStringToObject(b);
//                for (Map<String, Integer> row : table) {
//                    Integer val = row.get("v");
//                    frequencies.merge(val, 1, Integer::sum);
//                }
//            }
//        }
//        Optional<Map.Entry<Integer, Integer>> maxEntry =
//                frequencies.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1);
//        if (maxEntry.isPresent()) {
//            Pair<Integer, Integer> kf = new Pair<>(maxEntry.get().getKey(), maxEntry.get().getValue());
//            return Utilities.objectToByteString(kf);
//        } else {
//            return ByteString.EMPTY;
//        }
//    }
//
//    @Override
//    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
//        Integer maxKey = null;
//        int maxFrequency = Integer.MIN_VALUE;
//        for (ByteString b: shardQueryResults) {
//            if (!b.isEmpty()) {
//                Pair<Integer, Integer> kf = (Pair<Integer, Integer>) Utilities.byteStringToObject(b);
//                int key = kf.getValue0();
//                int frequency = kf.getValue1();
//                if (frequency > maxFrequency) {
//                    maxFrequency = frequency;
//                    maxKey = key;
//                }
//            }
//        }
//        return maxKey;
//    }
//
//    @Override
//    public List<AnchoredReadQueryPlan> getSubQueries() {return Collections.emptyList();}
//
//    @Override
//    public void setSubQueryResults(List<Object> subQueryResults) {}
//}

package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//public class TableReadPopularState implements AnchoredReadQueryPlan<TableShard, Integer> {
//
//    private static final Logger logger = LoggerFactory.getLogger(TableReadPopularState.class);
//
//    private final String tableOne;
//    private final String tableTwo;
//
//    public TableReadPopularState(String tableOne, String tableTwo) {
//        this.tableOne = tableOne;
//        this.tableTwo = tableTwo;
//    }
//
//    @Override
//    public List<String> getQueriedTables() {
//        return List.of(tableOne, tableTwo);
//    }
//
//    @Override
//    public Map<String, List<Integer>> keysForQuery() {
//        return Map.of(tableOne, List.of(-1), tableTwo, List.of(-1));
//    }
//
//    @Override
//    public String getAnchorTable() {
//        return Map.of(tableOne, true, tableTwo, true);
//    }
//
//    @Override
//    public ByteString queryShard(List<TableShard> shards) {
//        return null;
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
//            int partitionKey = ConsistentHash.hashFunction(row.get("city")) % numReducers;
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
//        Map<Integer, Integer> cityToState = new HashMap<>();
//        for(ByteString b: ephemeralData.get(tableTwo)) {
//            if (!b.isEmpty()) {
//                List<Map<String, Integer>> tableTwo = (List<Map<String, Integer>>) Utilities.byteStringToObject(b);
//                for (Map<String, Integer> row : tableTwo) {
//                    cityToState.put(row.get("city"), row.get("state"));
//                }
//            }
//        }
//        HashMap<Integer, Integer> stateFrequency = new HashMap<>();
//        for(ByteString b: ephemeralData.get(tableOne)) {
//            if (!b.isEmpty()) {
//                List<Map<String, Integer>> tableOne = (List<Map<String, Integer>>) Utilities.byteStringToObject(b);
//                for (Map<String, Integer> row : tableOne) {
//                    int state = cityToState.get(row.get("city"));
//                    stateFrequency.merge(state, 1, Integer::sum);
//                }
//            }
//        }
//        return Utilities.objectToByteString(stateFrequency);
//    }
//
//    @Override
//    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
//        Map<Integer, Integer> stateFrequency = new HashMap<>();
//        for (ByteString b: shardQueryResults) {
//            Map<Integer, Integer> shardStateFrequency = (HashMap<Integer, Integer>) Utilities.byteStringToObject(b);
//            shardStateFrequency.forEach((k, v) -> stateFrequency.merge(k, v, Integer::sum));
//        }
//        return stateFrequency.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1).get().getKey();
//    }
//
//    @Override
//    public List<AnchoredReadQueryPlan> getSubQueries() {return Collections.emptyList();}
//
//    @Override
//    public void setSubQueryResults(List<Object> subQueryResults) {}
//}

package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleReadQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableReadPopularState implements ShuffleReadQueryPlan<TableShard, Integer> {

    private static final Logger logger = LoggerFactory.getLogger(TableReadPopularState.class);

    private final String tableOne;
    private final String tableTwo;

    public TableReadPopularState(String tableOne, String tableTwo) {
        this.tableOne = tableOne;
        this.tableTwo = tableTwo;
    }

    @Override
    public List<String> getQueriedTables() {
        return List.of(tableOne, tableTwo);
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tableOne, List.of(-1), tableTwo, List.of(-1));
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(TableShard shard, int numRepartitions) {
        Map<Integer, ArrayList<Map<String, Integer>>> partitionedTables = new HashMap<>();
        for (Map<String, Integer> row: shard.table) {
            int partitionKey = ConsistentHash.hashFunction(row.get("city")) % numRepartitions;
            partitionedTables.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row);
        }
        HashMap<Integer, List<ByteString>> serializedTables = new HashMap<>();
        partitionedTables.forEach((k, v) -> serializedTables.put(k, List.of(
                Utilities.objectToByteString(new ArrayList<>(v.subList(0, 1))),
                Utilities.objectToByteString((new ArrayList<>(v.subList(1, v.size())))))));
        for (int i = 0; i < numRepartitions; i++) {
            if(!serializedTables.containsKey(i)) {
                serializedTables.put(i, List.of(ByteString.EMPTY));
            }
        }
        return serializedTables;
    }

    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, TableShard> ephemeralShards) {
        Map<Integer, Integer> cityToState = new HashMap<>();
        for(ByteString b: ephemeralData.get(tableTwo)) {
            if (!b.isEmpty()) {
                List<Map<String, Integer>> tableTwo = (List<Map<String, Integer>>) Utilities.byteStringToObject(b);
                for (Map<String, Integer> row : tableTwo) {
                    cityToState.put(row.get("city"), row.get("state"));
                }
            }
        }
        HashMap<Integer, Integer> stateFrequency = new HashMap<>();
        for(ByteString b: ephemeralData.get(tableOne)) {
            if (!b.isEmpty()) {
                List<Map<String, Integer>> tableOne = (List<Map<String, Integer>>) Utilities.byteStringToObject(b);
                for (Map<String, Integer> row : tableOne) {
                    int state = cityToState.get(row.get("city"));
                    stateFrequency.merge(state, 1, Integer::sum);
                }
            }
        }
        return Utilities.objectToByteString(stateFrequency);
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
}

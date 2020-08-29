package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class LoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);

    public static Integer epsilonRatio = 5;

    /**
     * Balance cluster load.
     * @param shardLoads Mapping from shard number to load.
     * @param consistentHash Coordinator consistent hash, whose reassignment map will be modified.
     * @return List of dsIDs that lost shards, list of dsIDs that gained shards.
     */
    public static Pair<Set<Integer>, Set<Integer>> balanceLoad(Map<Integer, Integer> shardLoads, ConsistentHash consistentHash) {
        return balanceLoad(shardLoads, consistentHash, null);
    }

    public static Pair<Set<Integer>, Set<Integer>> balanceLoad(Map<Integer, Integer> shardLoads, ConsistentHash consistentHash, Integer newServer) {
        Set<Integer> lostShards = new HashSet<>();
        Set<Integer> gainedShards = new HashSet<>();
        Map<Integer, Integer> serverLoads = new HashMap<>();
        Map<Integer, List<Integer>> serverToShards = new HashMap<>();
        serverLoads.putAll(consistentHash.buckets.stream().collect(Collectors.toMap(i -> i, i -> 0)));
        serverToShards.putAll(consistentHash.buckets.stream().collect(Collectors.toMap(i -> i, i -> new ArrayList<>())));
        for (int shardNum: shardLoads.keySet()) {
            int shardLoad = shardLoads.get(shardNum);
            int serverNum = consistentHash.getBucket(shardNum);
            serverLoads.merge(serverNum, shardLoad, Integer::sum);
            serverToShards.get(serverNum).add(shardNum);
        }
        PriorityQueue<Integer> serverMinQueue = new PriorityQueue<>(Comparator.comparing(serverLoads::get));
        PriorityQueue<Integer> serverMaxQueue = new PriorityQueue<>(Comparator.comparing(serverLoads::get).reversed());
        serverMinQueue.addAll(serverLoads.keySet());
        serverMaxQueue.addAll(serverLoads.keySet());
        double averageLoad = shardLoads.values().stream().mapToDouble(i -> i).sum() / serverLoads.size();
        double epsilon = averageLoad / epsilonRatio;
        while (serverMaxQueue.size() > 0 && serverLoads.get(serverMaxQueue.peek()) > averageLoad + epsilon) {
            Integer overLoadedServer = serverMaxQueue.remove();
            while (serverToShards.get(overLoadedServer).size() > 0 && serverLoads.get(overLoadedServer) > averageLoad + epsilon) {
                Integer underLoadedServer = serverMinQueue.remove();
                Integer mostLoadedShard = serverToShards.get(overLoadedServer).stream().filter(i -> shardLoads.get(i) > 0).max(Comparator.comparing(shardLoads::get)).orElse(null);
                assert(mostLoadedShard != null);
                serverToShards.get(overLoadedServer).remove(mostLoadedShard);
                if (serverLoads.get(underLoadedServer) + shardLoads.get(mostLoadedShard) <= averageLoad + epsilon &&
                        (newServer == null || overLoadedServer.equals(newServer) || underLoadedServer.equals(newServer))) {
                    consistentHash.reassignmentMap.put(mostLoadedShard, underLoadedServer);
                    serverLoads.merge(overLoadedServer, -1 * shardLoads.get(mostLoadedShard), Integer::sum);
                    serverLoads.merge(underLoadedServer, shardLoads.get(mostLoadedShard), Integer::sum);
                    lostShards.add(overLoadedServer);
                    gainedShards.add(underLoadedServer);
                    logger.info("Shard {} transferred from DS{} to DS{}", mostLoadedShard, overLoadedServer, underLoadedServer);
                }
                serverMinQueue.add(underLoadedServer);
            }
        }
        return new Pair<>(lostShards, gainedShards);
    }
}

package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DefaultLoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLoadBalancer.class);

    public static Integer epsilonRatio = 5;

    /**
     * Balance cluster load.
     * @param shardLoads A map from shard number to the load (queries per second) on that shard.
     * @param consistentHash The coordinator's consistent hash.  The load balancer will modify its reassignment map.
     * @param newServer Optional parameter.  If set, shards will only be added to or removed from the specified server.
     * @return A pair of lists of servers:  first those that lost shards in balancing, then those that gained shards.
     */
    public static Pair<Set<Integer>, Set<Integer>> balanceLoad(Map<Integer, Integer> shardLoads, ConsistentHash consistentHash) {
        return balanceLoad(shardLoads, consistentHash, null);
    }

    public static Pair<Set<Integer>, Set<Integer>> balanceLoad(Map<Integer, Integer> shardLoads,
                                                               ConsistentHash consistentHash, Integer newServer) {
        Set<Integer> lostShards = new HashSet<>();
        Set<Integer> gainedShards = new HashSet<>();
        Map<Integer, Integer> serverLoads = new HashMap<>();
        Map<Integer, List<Integer>> serverToShards = new HashMap<>();
        serverLoads.putAll(consistentHash.buckets.stream().collect(Collectors.toMap(i -> i, i -> 0)));
        serverToShards.putAll(consistentHash.buckets.stream().collect(Collectors.toMap(i -> i, i -> new ArrayList<>())));
        for (int shardNum: shardLoads.keySet()) {
            int shardLoad = shardLoads.get(shardNum);
            List<Integer> serverNums = consistentHash.getBuckets(shardNum);
            for (Integer serverNum: serverNums) {
                serverLoads.merge(serverNum, shardLoad / serverNums.size(), Integer::sum);
                serverToShards.get(serverNum).add(shardNum);
            }
        }
        PriorityQueue<Integer> serverMinQueue = new PriorityQueue<>(Comparator.comparing(serverLoads::get));
        serverMinQueue.addAll(serverLoads.keySet());
        PriorityQueue<Integer> serverMaxQueue = new PriorityQueue<>(Comparator.comparing(serverLoads::get).reversed());
        serverMaxQueue.addAll(serverLoads.keySet());

        double averageLoad = shardLoads.values().stream().mapToDouble(i -> i).sum() / serverLoads.size();
        double epsilon = averageLoad / epsilonRatio;
        while (serverMaxQueue.size() > 0 && serverLoads.get(serverMaxQueue.peek()) > averageLoad + epsilon) {
            Integer overLoadedServer = serverMaxQueue.remove();
            while (serverToShards.get(overLoadedServer).stream().anyMatch(i -> shardLoads.get(i) > 0)
                    && serverLoads.get(overLoadedServer) > averageLoad + epsilon) {
                Integer underLoadedServer;
                if (newServer == null || serverLoads.get(newServer) > averageLoad + epsilon) {
                    underLoadedServer = serverMinQueue.remove();
                } else {
                    underLoadedServer = newServer;
                }
                Integer mostLoadedShard = serverToShards.get(overLoadedServer).stream().filter(i -> shardLoads.get(i) > 0)
                        .max(Comparator.comparing(i -> shardLoads.get(i) / consistentHash.getBuckets(i).size())).orElse(null);
                assert(mostLoadedShard != null);
                int mostLoadedShardLoad = shardLoads.get(mostLoadedShard) / consistentHash.getBuckets(mostLoadedShard).size();
                serverToShards.get(overLoadedServer).remove(mostLoadedShard);
                if (serverLoads.get(underLoadedServer) + mostLoadedShardLoad <= averageLoad + epsilon &&
                        (newServer == null || overLoadedServer.equals(newServer) || underLoadedServer.equals(newServer))) {
                    if (consistentHash.reassignmentMap.containsKey(mostLoadedShard)) {
                        consistentHash.reassignmentMap.get(mostLoadedShard).remove(overLoadedServer);
                        consistentHash.reassignmentMap.get(mostLoadedShard).add(underLoadedServer);
                    } else {
                        consistentHash.reassignmentMap.put(mostLoadedShard, new ArrayList<>(List.of(underLoadedServer)));
                    }
                    serverLoads.merge(overLoadedServer, -1 * mostLoadedShardLoad, Integer::sum);
                    serverLoads.merge(underLoadedServer, mostLoadedShardLoad, Integer::sum);
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

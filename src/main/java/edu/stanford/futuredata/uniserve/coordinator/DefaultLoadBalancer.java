package edu.stanford.futuredata.uniserve.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DefaultLoadBalancer implements LoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLoadBalancer.class);

    public static Integer epsilonRatio = 5;

    /**
     * Balance cluster load.
     */
    public Map<Integer, Integer> balanceLoad(Set<Integer> shards, Set<Integer> servers,
                                                               Map<Integer, Integer> shardLoads,
                                                               Map<Integer, Integer> currentLocations) {
        Map<Integer, Integer> updatedLocations = new HashMap<>(currentLocations);
        Map<Integer, Integer> serverLoads = new HashMap<>();
        Map<Integer, List<Integer>> serverToShards = new HashMap<>();
        serverLoads.putAll(servers.stream().collect(Collectors.toMap(i -> i, i -> 0)));
        serverToShards.putAll(servers.stream().collect(Collectors.toMap(i -> i, i -> new ArrayList<>())));
        for (int shardNum: shards) {
            int shardLoad = shardLoads.get(shardNum);
            int serverNum = currentLocations.get(shardNum);
            serverLoads.merge(serverNum, shardLoad, Integer::sum);
            serverToShards.get(serverNum).add(shardNum);
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
                underLoadedServer = serverMinQueue.remove();
                Integer mostLoadedShard = serverToShards.get(overLoadedServer).stream().filter(i -> shardLoads.get(i) > 0)
                        .max(Comparator.comparing(shardLoads::get)).orElse(null);
                assert(mostLoadedShard != null);
                int mostLoadedShardLoad = shardLoads.get(mostLoadedShard);
                serverToShards.get(overLoadedServer).remove(mostLoadedShard);
                if (serverLoads.get(underLoadedServer) + mostLoadedShardLoad <= averageLoad + epsilon) {
                    assert(updatedLocations.get(mostLoadedShard).equals(overLoadedServer));
                    updatedLocations.put(mostLoadedShard, underLoadedServer);
                    serverLoads.merge(overLoadedServer, -1 * mostLoadedShardLoad, Integer::sum);
                    serverLoads.merge(underLoadedServer, mostLoadedShardLoad, Integer::sum);
                    logger.info("Shard {} transferred from DS{} to DS{}", mostLoadedShard, overLoadedServer, underLoadedServer);
                }
                serverMinQueue.add(underLoadedServer);
            }
        }
        return updatedLocations;
    }
}

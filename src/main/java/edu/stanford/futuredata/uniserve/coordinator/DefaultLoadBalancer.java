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

        // Replication.
        int averageServerLoad = shardLoads.values().stream().mapToInt(i -> i).sum() / consistentHash.buckets.size();
        for (Map.Entry<Integer, Integer> shardLoad: shardLoads.entrySet()) {
            Integer shardNum = shardLoad.getKey();
            int load = shardLoad.getValue();
            int targetReplicas = (load % averageServerLoad == 0) ? load / averageServerLoad :
                    load / averageServerLoad + 1;
            targetReplicas = (targetReplicas == 0) ? 1 : targetReplicas;
            targetReplicas = Math.min(targetReplicas, serverLoads.size());
            int numReplicas = consistentHash.reassignmentMap.containsKey(shardNum) ?
                    consistentHash.reassignmentMap.get(shardNum).size() : 1;
            if (numReplicas < targetReplicas) {
                // Add replica
                Integer targetServer = null;
                while (targetServer == null && !serverMinQueue.isEmpty()) {
                    Integer server = serverMinQueue.remove();
                    if (!serverToShards.get(server).contains(shardNum) && !lostShards.contains(server)) {
                        targetServer = server;
                    }
                }
                if (targetServer == null) {
                    continue;
                }
                logger.info("Add Replica of Shard {} on DS{}", shardNum, targetServer);
                serverToShards.get(targetServer).add(shardNum);
                List<Integer> currentlyAssigned = consistentHash.getBuckets(shardNum);
                int originallyAssigned = currentlyAssigned.size();
                if (consistentHash.reassignmentMap.containsKey(shardNum)) {
                    consistentHash.reassignmentMap.get(shardNum).add(targetServer);
                } else {
                    consistentHash.reassignmentMap.put(shardNum,
                            new ArrayList<>(List.of(targetServer, consistentHash.getRandomBucket(shardNum))));
                }
                Integer targetServerLoad = serverLoads.get(targetServer);
                serverLoads.put(targetServer, targetServerLoad + load / (originallyAssigned + 1));
                gainedShards.add(targetServer);
                serverMaxQueue.remove(targetServer);
                serverMaxQueue.add(targetServer);
                serverMinQueue.add(targetServer);
                for (Integer currentServer: currentlyAssigned) {
                    Integer currentServerLoad = serverLoads.get(currentServer);
                    int loadChange = load / (originallyAssigned + 1) - load / originallyAssigned;
                    serverLoads.put(currentServer, currentServerLoad + loadChange);
                    serverMaxQueue.remove(currentServer);
                    serverMaxQueue.add(currentServer);
                    serverMinQueue.remove(currentServer);
                    serverMinQueue.add(currentServer);
                }
            } else if (numReplicas > targetReplicas) {
                // Remove replica
                Integer targetServer = null;
                List<Integer> removedServers = new ArrayList<>();
                while (targetServer == null && !serverMaxQueue.isEmpty()) {
                    Integer server = serverMaxQueue.remove();
                    if (serverToShards.get(server).contains(shardNum) && !gainedShards.contains(server)) {
                        targetServer = server;
                    } else {
                        removedServers.add(server);
                    }
                }
                if (targetServer == null) {
                    continue;
                }
                logger.info("Remove Replica of Shard {} from DS{}", shardNum, targetServer);
                serverMaxQueue.addAll(removedServers);
                serverToShards.get(targetServer).remove(shardNum);
                List<Integer> currentlyAssigned = consistentHash.getBuckets(shardNum);
                int originallyAssigned = currentlyAssigned.size();
                assert(originallyAssigned > 1);
                assert(consistentHash.reassignmentMap.containsKey(shardNum));
                consistentHash.reassignmentMap.get(shardNum).remove(targetServer);
                assert(consistentHash.reassignmentMap.get(shardNum).size() > 0);
                Integer targetServerLoad = serverLoads.get(targetServer);
                serverLoads.put(targetServer, targetServerLoad - load / originallyAssigned);
                lostShards.add(targetServer);
                serverMinQueue.remove(targetServer);
                serverMinQueue.add(targetServer);
                serverMaxQueue.add(targetServer);
                for (Integer currentServer: currentlyAssigned) {
                    Integer currentServerLoad = serverLoads.get(currentServer);
                    int loadChange =  load / (originallyAssigned - 1) - load / originallyAssigned;
                    serverLoads.put(currentServer, currentServerLoad + loadChange);
                    serverMaxQueue.remove(currentServer);
                    serverMaxQueue.add(currentServer);
                    serverMinQueue.remove(currentServer);
                    serverMinQueue.add(currentServer);
                }
            }
        }

        // Balancing.
        serverMinQueue.clear();
        serverMinQueue.addAll(serverLoads.keySet().stream().filter(i -> !lostShards.contains(i)).collect(Collectors.toList()));
        serverMaxQueue.clear();
        serverMaxQueue.addAll(serverLoads.keySet().stream().filter(i -> !gainedShards.contains(i)).collect(Collectors.toList()));

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

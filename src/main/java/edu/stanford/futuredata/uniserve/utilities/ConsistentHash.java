package edu.stanford.futuredata.uniserve.utilities;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A consistent hashing function that assigns shards to servers (buckets).
 * Consistent hashing assignments can be overriden by the "reassignment map", modified by the load balancer.
 */
public class ConsistentHash implements Serializable {

    private final List<Integer> hashRing = new ArrayList<>();
    private final Map<Integer, Integer> hashToBucket = new HashMap<>();

    private static final int numVirtualNodes = 10;
    private static final int virtualOffset = 1234567;

    private static final double A = (Math.sqrt(5) - 1) / 2;
    private static final int m = 2147483647; // 2 ^ 31 - 1

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    // A mapping of keys reassigned away from their consistent-hash buckets.
    public final Map<Integer, List<Integer>> reassignmentMap = new HashMap<>();

    public final Set<Integer> buckets = new HashSet<>();

    public static int hashFunction(int k) {
        // from CLRS, including the magic numbers.
        return (int) (m * (k * A - Math.floor(k * A)));
    }

    // Add a bucket (server) to the consistent hash.
    public void addBucket(int bucketNum) {
        lock.writeLock().lock();
        for(int i = 0; i < numVirtualNodes; i++) {
            int hash = hashFunction(bucketNum + virtualOffset * i);
            hashRing.add(hash);
            hashToBucket.put(hash, bucketNum);
        }
        Collections.sort(hashRing);
        buckets.add(bucketNum);
        lock.writeLock().unlock();
    }

    // Remove a bucket (server) from the consistent hash.
    public void removeBucket (Integer bucketNum) {
        lock.writeLock().lock();
        assert(buckets.contains(bucketNum));
        List<Integer> toRemove = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> e: reassignmentMap.entrySet()) {
            List<Integer> replicasList = e.getValue();
            replicasList.remove(bucketNum);
            if (replicasList.size() == 0) {
                toRemove.add(e.getKey());
            }
        }
        toRemove.forEach(reassignmentMap::remove);
        for(int i = 0; i < numVirtualNodes; i++) {
            Integer hash = hashFunction(bucketNum + virtualOffset * i);
            hashRing.remove(hash);
            hashToBucket.remove(hash);
        }
        Collections.sort(hashRing);
        buckets.remove(bucketNum);
        lock.writeLock().unlock();
    }

    // Get all buckets a key (shard) is assigned to.  By default, a shard is only assigned to one server, but
    // load balancer replication may assign it to more.
    public List<Integer> getBuckets(int key) {
        if (reassignmentMap.containsKey(key)) {
            return reassignmentMap.get(key);
        }
        lock.readLock().lock();
        int hash = hashFunction(key);
        for (int n : hashRing) {
            if (hash < n) {
                int ret = hashToBucket.get(n);
                lock.readLock().unlock();
                return List.of(ret);
            }
        }
        int ret = hashToBucket.get(hashRing.get(0));
        lock.readLock().unlock();
        return List.of(ret);
    }

    // Get a random bucket (server) to which a key (shard) is assigned.
    public Integer getRandomBucket(int key) {
        if (reassignmentMap.containsKey(key)) {
            List<Integer> keys = reassignmentMap.get(key);
            return keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
        }
        lock.readLock().lock();
        int hash = hashFunction(key);
        for (int n : hashRing) {
            if (hash < n) {
                int ret = hashToBucket.get(n);
                lock.readLock().unlock();
                return ret;
            }
        }
        int ret = hashToBucket.get(hashRing.get(0));
        lock.readLock().unlock();
        return ret;
    }

}

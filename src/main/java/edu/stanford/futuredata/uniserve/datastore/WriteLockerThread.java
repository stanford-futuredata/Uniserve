package edu.stanford.futuredata.uniserve.datastore;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

// Holds a shard's write lock during 2PC.
class WriteLockerThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(WriteLockerThread.class);
    private final ShardLock lock;
    private final PreemptibleStreamObserver owner;
    private final Pair<Integer, Integer> ownerID;
    private final long txID;
    private final Semaphore acquireLockSemaphore = new Semaphore(0);
    private final Semaphore releaseLockSemaphore = new Semaphore(0);

    private final static Map<Pair<Integer, Integer>, PreemptibleStreamObserver> ownerMap = new HashMap<>();

    public WriteLockerThread(ShardLock lock, PreemptibleStreamObserver owner, int dsID, int shardNum, long txID) {
        this.lock = lock;
        this.owner = owner;
        this.ownerID = new Pair<>(dsID, shardNum);
        this.txID = txID;
    }

    public void run() {
        PreemptibleStreamObserver currentOwner = null;
        boolean acquired =  lock.writerLockTryLock();
        if (!acquired) {
            currentOwner = ownerMap.get(ownerID);
            assert(currentOwner != null);
            if (currentOwner.getTXID() < txID) {
                // If the current transaction has a lower ID, do not preempt.
                lock.writerLockLock();
                currentOwner = null;
            } else {
                // If it has a higher ID, preempt it.
                currentOwner.preempt();
            }
        }
        acquireLockSemaphore.release();
        try {
            releaseLockSemaphore.acquire();
        } catch (InterruptedException e) {
            logger.error("DS Interrupted while getting lock: {}", e.getMessage());
            assert(false);
        }
        if (currentOwner == null) {
            lock.writerLockUnlock();
        } else {
            currentOwner.resume();
        }
    }

    public void acquireLock() {
        this.start();
        try {
            acquireLockSemaphore.acquire();
            ownerMap.put(ownerID, owner);
        } catch (InterruptedException e) {
            logger.error("DS Interrupted while getting lock: {}", e.getMessage());
            assert(false);
        }
    }

    public void releaseLock() {
        assert(this.isAlive());
        ownerMap.put(ownerID, null);
        releaseLockSemaphore.release();
        try {
            this.join();
        } catch (InterruptedException ignored) {}
    }
}
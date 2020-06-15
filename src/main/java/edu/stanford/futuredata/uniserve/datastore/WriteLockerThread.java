package edu.stanford.futuredata.uniserve.datastore;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    private final static Map<Pair<Integer, Integer>, PreemptibleStreamObserver> ownerMap = new ConcurrentHashMap<>();

    public WriteLockerThread(ShardLock lock, PreemptibleStreamObserver owner, int dsID, int shardNum, long txID) {
        this.lock = lock;
        this.owner = owner;
        this.ownerID = new Pair<>(dsID, shardNum);
        this.txID = txID;
    }

    public void run() {
        lock.addWaitingTXID(txID);
        PreemptibleStreamObserver currentOwner = null;
        boolean acquired =  false;
        while (!acquired && currentOwner == null) {
            acquired = lock.writerLockTryLock(txID);
            currentOwner = ownerMap.get(ownerID);
        }
        if (!acquired) {
            if (currentOwner.getTXID() < txID) {
                // If the current transaction has a lower ID, do not preempt.
                lock.writerLockLock(txID);
                acquired = true;
            } else {
                // If it has a higher ID, preempt it.
                boolean preempted = currentOwner.preempt();
                if (!preempted) {
                    lock.writerLockLock(txID);
                    acquired = true;
                }
            }
        }
        lock.removeWaitingTXID(txID);
        acquireLockSemaphore.release();
        try {
            releaseLockSemaphore.acquire();
        } catch (InterruptedException e) {
            logger.error("DS Interrupted while getting lock: {}", e.getMessage());
            assert(false);
        }
        if (acquired) {
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
        ownerMap.remove(ownerID);
        releaseLockSemaphore.release();
        try {
            this.join();
        } catch (InterruptedException ignored) {}
    }
}
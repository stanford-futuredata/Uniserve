package edu.stanford.futuredata.uniserve.datastore;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

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
        PreemptibleStreamObserver formerOwner = null;
        while (true) {  // TODO:  Don't spin.
            if (lock.writerLockTryLock(txID)) {
                break;
            }
            PreemptibleStreamObserver currentOwner = ownerMap.get(ownerID);
            if (currentOwner != null && txID < currentOwner.getTXID()) {
                boolean preempted = currentOwner.preempt();
                if (preempted) {
                    formerOwner = currentOwner;
                    break;
                }
            }
        }
        ownerMap.put(ownerID, owner);
        lock.removeWaitingTXID(txID);
        acquireLockSemaphore.release();
        try {
            releaseLockSemaphore.acquire();
        } catch (InterruptedException e) {
            logger.error("DS Interrupted while getting lock: {}", e.getMessage());
            assert(false);
        }
        if (formerOwner == null) {
            ownerMap.remove(ownerID);
            lock.writerLockUnlock();
        } else {
            ownerMap.put(ownerID, formerOwner);
            formerOwner.resume();
        }
    }

    public void acquireLock() {
        this.start();
        try {
            acquireLockSemaphore.acquire();
        } catch (InterruptedException e) {
            logger.error("DS Interrupted while getting lock: {}", e.getMessage());
            assert(false);
        }
    }

    public void releaseLock() {
        assert(this.isAlive());
        releaseLockSemaphore.release();
        try {
            this.join();
        } catch (InterruptedException ignored) {}
    }
}
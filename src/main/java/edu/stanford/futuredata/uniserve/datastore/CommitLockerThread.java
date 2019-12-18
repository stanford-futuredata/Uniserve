package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

class CommitLockerThread<R extends Row, S extends Shard> extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(CommitLockerThread.class);
    // Holds a shard's write lock in between precommit and commit.
    public final Integer shardNum;
    public final WriteQueryPlan<R, S> writeQueryPlan;
    public final List<R> rows;
    private final Lock lock;
    private final Map<Integer, CommitLockerThread<R, S>> activeCLTs;
    public final long txID;
    private final long dsID;
    private Semaphore acquireLockSemaphore = new Semaphore(0);
    private Semaphore releaseLockSemaphore = new Semaphore(0);

    public CommitLockerThread(Map<Integer, CommitLockerThread<R, S>> activeCLTs, Integer shardNum, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows, Lock lock, long txID, int dsID) {
        this.activeCLTs = activeCLTs;
        this.shardNum = shardNum;
        this.writeQueryPlan = writeQueryPlan;
        this.rows = rows;
        this.lock = lock;
        this.txID = txID;
        this.dsID = dsID;
    }

    public void run() {
        lock.lock();
        assert (activeCLTs.get(shardNum) == null);
        activeCLTs.put(shardNum, this);
        // Notify the precommit thread that the shard lock is held.
        acquireLockSemaphore.release();
        // Block until the commit thread is ready to release the shard lock.
        // TODO:  Automatically abort and unlock if this isn't triggered for X seconds after a precommit.
        try {
            releaseLockSemaphore.acquire();
        } catch (InterruptedException e) {
            logger.error("DS{} Interrupted while getting lock: {}", dsID, e.getMessage());
            assert(false);
        }
        lock.unlock();
    }

    public void acquireLock() {
        this.start();
        try {
            acquireLockSemaphore.acquire();
        } catch (InterruptedException e) {
            logger.error("DS{} Interrupted while getting lock: {}", dsID, e.getMessage());
            assert(false);
        }
    }

    public void releaseLock() {
        assert(this.isAlive());
        assert(activeCLTs.get(shardNum) == this);
        activeCLTs.put(shardNum, null);
        releaseLockSemaphore.release();
        try {
            this.join();
        } catch (InterruptedException ignored) {}
    }
}

package edu.stanford.futuredata.uniserve.datastore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

// Holds a shard's write lock during 2PC.
class WriteLockerThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(WriteLockerThread.class);
    private final ShardLock lock;
    private final Semaphore acquireLockSemaphore = new Semaphore(0);
    private final Semaphore releaseLockSemaphore = new Semaphore(0);

    public WriteLockerThread(ShardLock lock) {
        this.lock = lock;
    }

    public void run() {
        lock.writerLockLock();
        acquireLockSemaphore.release();
        releaseLockSemaphore.acquireUninterruptibly();
        lock.writerLockUnlock();
    }

    public void acquireLock() {
        this.start();
        acquireLockSemaphore.acquireUninterruptibly();
    }

    public void releaseLock() {
        assert(this.isAlive());
        releaseLockSemaphore.release();
        try {
            this.join();
        } catch (InterruptedException ignored) {}
    }
}
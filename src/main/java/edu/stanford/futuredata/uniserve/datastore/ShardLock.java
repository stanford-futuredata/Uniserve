package edu.stanford.futuredata.uniserve.datastore;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardLock {
    private final ReadWriteLock systemLock = new ReentrantReadWriteLock();
    private final Lock writerLock = new ReentrantLock();

    private enum Mode {
        SYSTEM, WRITE, READ
    }

    private Mode mode = null;

    public void systemLockLock() {
        systemLock.writeLock().lock();
        mode = Mode.SYSTEM;
    }

    public void systemLockUnlock() {
        assert(mode == Mode.SYSTEM);
        mode = null;
        systemLock.writeLock().unlock();
    }

    public void writerLockLock() {
        systemLock.readLock().lock();
        writerLock.lock();
        mode = Mode.WRITE;
    }

    public void writerLockUnlock() {
        assert(mode == Mode.WRITE);
        mode = null;
        writerLock.unlock();
        systemLock.readLock().unlock();
    }

    public void readerLockLock() {
        systemLock.readLock().lock();
        mode = Mode.READ;
    }

    public void readerLockUnlock() {
        assert(mode == Mode.READ);
        mode = null;
        systemLock.readLock().unlock();
    }
}

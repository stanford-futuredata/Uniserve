package edu.stanford.futuredata.uniserve.datastore;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardLock {
    private final ReadWriteLock systemLock = new ReentrantReadWriteLock();
    private final Lock writerLock = new ReentrantLock();

    private final Set<Long> waitingTXIDs = new CopyOnWriteArraySet<>();
    private long minWaitingTXID = Long.MAX_VALUE;

    private enum Mode {
        SYSTEM, WRITE, READ
    }

    private Mode mode = null;

    public void systemLockLock() {
        systemLock.writeLock().lock();
        assert(Objects.isNull(mode));
        mode = Mode.SYSTEM;
    }

    public void systemLockUnlock() {
        assert(mode == Mode.SYSTEM);
        mode = null;
        systemLock.writeLock().unlock();
    }

    public void writerLockLock(long txID) {
        systemLock.readLock().lock();
        while (txID > minWaitingTXID) {
            try {
                Thread.sleep(1); // TODO:  Something smarter here.
            } catch (InterruptedException ignored) {}
        }
        writerLock.lock();
        assert(Objects.isNull(mode));
        mode = Mode.WRITE;
    }

    public boolean writerLockTryLock(long txID) {
        systemLock.readLock().lock();
        if (txID <= minWaitingTXID && writerLock.tryLock()) {
            mode = Mode.WRITE;
            return true;
        } else {
            systemLock.readLock().unlock();
            return false;
        }
    }

    public void addWaitingTXID(long txID) {
        assert(!waitingTXIDs.contains(txID));
        waitingTXIDs.add(txID);
        minWaitingTXID = waitingTXIDs.stream().min(Long::compare).orElse(Long.MAX_VALUE);
    }

    public void removeWaitingTXID(long txID) {
        assert(waitingTXIDs.contains(txID));
        waitingTXIDs.remove(txID);
        minWaitingTXID = waitingTXIDs.stream().min(Long::compare).orElse(Long.MAX_VALUE);
    }

    public void writerLockUnlock() {
        assert(mode == Mode.WRITE);
        mode = null;
        writerLock.unlock();
        systemLock.readLock().unlock();
    }

    public void readerLockLock() {
        systemLock.readLock().lock();
    }

    public void readerLockUnlock() {
        systemLock.readLock().unlock();
    }
}

package edu.stanford.futuredata.uniserve.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Row;

public class KVRow implements Row {
    private final int key;
    private final int value;
    private final long timestamp;
    public KVRow(int key, int value) {
        this.key = key;
        this.value = value;
        this.timestamp = 0;
    }

    public KVRow(int key, int value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public int getPartitionKey() {
        return this.key;
    }

    @Override
    public long getTimeStamp() {
        return timestamp;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}

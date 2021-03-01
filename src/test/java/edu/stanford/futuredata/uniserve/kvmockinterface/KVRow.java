package edu.stanford.futuredata.uniserve.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Row;

public class KVRow implements Row {
    private final int key;
    private final int value;

    public KVRow(int key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int getPartitionKey() {
        return this.key;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}

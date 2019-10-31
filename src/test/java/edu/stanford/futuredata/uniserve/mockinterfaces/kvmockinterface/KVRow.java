package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Row;

public class KVRow implements Row {
    private int key;
    private int value;
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

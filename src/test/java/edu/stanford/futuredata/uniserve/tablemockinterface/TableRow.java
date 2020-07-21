package edu.stanford.futuredata.uniserve.tablemockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Row;

import java.util.Map;

public class TableRow implements Row {
    private final int key;
    private final Map<String, Integer> row;
    public TableRow(Map<String, Integer> row, int key) {
        this.key = key;
        this.row = row;
    }

    @Override
    public int getPartitionKey() {
        return this.key;
    }

    @Override
    public long getTimeStamp() {
        return 1;
    }

    public Map<String, Integer> getRow() {
        return row;
    }
}

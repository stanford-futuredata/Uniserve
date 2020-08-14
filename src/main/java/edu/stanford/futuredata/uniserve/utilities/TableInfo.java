package edu.stanford.futuredata.uniserve.utilities;

public class TableInfo {
    public final String name;
    public final Integer id;
    public final Integer numShards;

    public TableInfo(String name, Integer id, Integer numShards) {
        this.name = name;
        this.id = id;
        this.numShards = numShards;
    }
}


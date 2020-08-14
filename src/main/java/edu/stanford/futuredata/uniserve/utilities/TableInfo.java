package edu.stanford.futuredata.uniserve.utilities;

import com.google.protobuf.ByteString;

public class TableInfo {
    public final String name;
    public final Integer id;
    public final Integer numShards;
    public final ByteString schema;

    public TableInfo(String name, Integer id, Integer numShards, ByteString schema) {
        this.name = name;
        this.id = id;
        this.numShards = numShards;
        this.schema = schema;
    }
}


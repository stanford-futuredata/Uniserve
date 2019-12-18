package edu.stanford.futuredata.uniserve.utilities;

import java.util.concurrent.atomic.AtomicInteger;

public class DataStoreDescription {

    public static final int ALIVE = 0;
    public static final int DEAD = 1;

    public final int dsID;
    public AtomicInteger status;
    public final String host;
    public final int port;

    public final String summaryString;

    public DataStoreDescription(int dsID, int status, String host, int port) {
        this.host = host;
        this.port = port;
        this.dsID = dsID;
        assert(status == ALIVE || status == DEAD);
        this.status = new AtomicInteger(status);
        this.summaryString = String.format("%d\n%d\n%s\n%d", dsID, status, host, port);
    }

    public DataStoreDescription(String summaryString) {
        this.summaryString = summaryString;
        String[] dsValues = summaryString.split("\n");
        dsID = Integer.parseInt(dsValues[0]);
        status = new AtomicInteger(Integer.parseInt(dsValues[1]));
        assert(status.get() == ALIVE || status.get() == DEAD);
        host = dsValues[2];
        port = Integer.parseInt(dsValues[3]);
    }
}

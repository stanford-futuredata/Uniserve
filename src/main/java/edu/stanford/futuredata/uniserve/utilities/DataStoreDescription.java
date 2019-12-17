package edu.stanford.futuredata.uniserve.utilities;

public class DataStoreDescription {

    public static final int ALIVE = 0;
    public static final int DEAD = 1;

    public final String host;
    public final int port;
    public final String connectString;
    public final int dsID;
    public final int status;
    public final String summaryString;

    public DataStoreDescription(int dsID, int status, String host, int port) {
        this.host = host;
        this.port = port;
        this.connectString = String.format("%s:%d", host, port);
        this.dsID = dsID;
        assert(status == ALIVE || status == DEAD);
        this.status = status;
        this.summaryString = String.format("%d\n%d\n%s\n%d", dsID, status, host, port);
    }

    public DataStoreDescription(String summaryString) {
        this.summaryString = summaryString;
        String[] dsValues = summaryString.split("\n");
        dsID = Integer.parseInt(dsValues[0]);
        status = Integer.parseInt(dsValues[1]);
        assert(status == ALIVE || status == DEAD);
        host = dsValues[2];
        port = Integer.parseInt(dsValues[3]);
        this.connectString = String.format("%s:%d", host, port);
    }
}

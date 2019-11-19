package edu.stanford.futuredata.uniserve.datastore;

public class ZKShardDescription {
    public final String connectString;
    public final String cloudName;
    public final int versionNumber;
    public final String stringSummary;

    public ZKShardDescription(String connectString, String cloudName, int versionNumber) {
        this.connectString = connectString;
        this.cloudName = cloudName;
        this.versionNumber = versionNumber;

        this.stringSummary = String.format("%s@@@%s@@@%d", connectString, cloudName, versionNumber);
    }

    public ZKShardDescription(String stringSummary) {
        this.stringSummary = stringSummary;
        String[] ccv = stringSummary.split("@@@");
        assert(ccv.length == 3);
        connectString = ccv[0];
        cloudName = ccv[1];
        versionNumber = Integer.parseInt(ccv[2]);
    }
}

package edu.stanford.futuredata.uniserve.datastore;

public class ZKShardDescription {
    public final String primaryConnectString;
    public final String cloudName;
    public final int versionNumber;
    public final String stringSummary;

    public ZKShardDescription(String primaryConnectString, String cloudName, int versionNumber) {
        this.primaryConnectString = primaryConnectString;
        this.cloudName = cloudName;
        this.versionNumber = versionNumber;

        stringSummary = String.format("%s\n%d\n%s\n", cloudName, versionNumber, primaryConnectString);
    }

    public ZKShardDescription(String stringSummary) {
        this.stringSummary = stringSummary;
        String[] ccv = stringSummary.split("\n");
        assert(ccv.length >= 3);
        cloudName = ccv[0];
        versionNumber = Integer.parseInt(ccv[1]);
        primaryConnectString = ccv[2];
    }
}

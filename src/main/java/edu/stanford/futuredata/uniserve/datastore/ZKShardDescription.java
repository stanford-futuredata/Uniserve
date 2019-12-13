package edu.stanford.futuredata.uniserve.datastore;

public class ZKShardDescription {
    public final int primaryDSID;
    public final String cloudName;
    public final int versionNumber;
    public final String stringSummary;

    public ZKShardDescription(int primaryDSID, String cloudName, int versionNumber) {
        this.cloudName = cloudName;
        this.versionNumber = versionNumber;
        this.primaryDSID = primaryDSID;
        stringSummary = String.format("%d\n%s\n%d\n", primaryDSID, cloudName, versionNumber);
    }

    public ZKShardDescription(String stringSummary) {
        this.stringSummary = stringSummary;
        String[] ccv = stringSummary.split("\n");
        assert(ccv.length >= 3);
        primaryDSID = Integer.parseInt(ccv[0]);
        cloudName = ccv[1];
        versionNumber = Integer.parseInt(ccv[2]);
    }
}

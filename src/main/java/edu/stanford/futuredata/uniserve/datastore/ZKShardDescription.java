package edu.stanford.futuredata.uniserve.datastore;

import java.util.ArrayList;
import java.util.List;

public class ZKShardDescription {
    public final String primaryConnectString;
    public final List<String> secondaryConnectStrings;
    public final String cloudName;
    public final int versionNumber;
    public final String stringSummary;

    public ZKShardDescription(String primaryConnectString, String cloudName, int versionNumber, List<String> secondaryConnectStrings) {
        this.primaryConnectString = primaryConnectString;
        this.cloudName = cloudName;
        this.versionNumber = versionNumber;
        this.secondaryConnectStrings = secondaryConnectStrings;

        StringBuilder stringSummaryBuilder =
                new StringBuilder(String.format("%s\n%d\n%s\n", cloudName, versionNumber, primaryConnectString));
        for (String secondaryConnectString: secondaryConnectStrings) {
            stringSummaryBuilder.append(secondaryConnectString);
        }
        stringSummary = stringSummaryBuilder.toString();
    }

    public ZKShardDescription(String stringSummary) {
        this.stringSummary = stringSummary;
        String[] ccv = stringSummary.split("\n");
        assert(ccv.length >= 3);
        cloudName = ccv[0];
        versionNumber = Integer.parseInt(ccv[1]);
        primaryConnectString = ccv[2];
        secondaryConnectStrings = new ArrayList<>();
        for (int i = 3; i < ccv.length; i++) {
            secondaryConnectStrings.add(ccv[i]);
        }
    }
}

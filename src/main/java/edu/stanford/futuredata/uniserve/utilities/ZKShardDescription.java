package edu.stanford.futuredata.uniserve.utilities;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ZKShardDescription {
    public final String cloudName;
    public final int versionNumber;
    public final String stringSummary;

    public ZKShardDescription(String cloudName, int versionNumber) {
        this.cloudName = cloudName;
        this.versionNumber = versionNumber;
        stringSummary = String.format("%s\n%d\n", cloudName, versionNumber);
    }

    public ZKShardDescription(String stringSummary) {
        this.stringSummary = stringSummary;
        String[] ccv = stringSummary.split("\n");
        assert(ccv.length == 2);
        cloudName = ccv[0];
        versionNumber = Integer.parseInt(ccv[1]);
    }
}

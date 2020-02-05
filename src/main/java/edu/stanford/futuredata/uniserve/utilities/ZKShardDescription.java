package edu.stanford.futuredata.uniserve.utilities;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ZKShardDescription {
    public final int primaryDSID;
    public final String cloudName;
    public final int versionNumber;
    public final String stringSummary;
    public final List<Integer> replicaDSIDs;
    public final List<Double> replicaRatios;

    public ZKShardDescription(int primaryDSID, String cloudName, int versionNumber, List<Integer> replicaDSIDs, List<Double> replicaRatios) {
        assert(replicaDSIDs.size() == replicaRatios.size());
        this.cloudName = cloudName;
        this.versionNumber = versionNumber;
        this.primaryDSID = primaryDSID;
        this.replicaDSIDs = replicaDSIDs;
        this.replicaRatios = replicaRatios;
        String primaryString = String.format("%d\n%s\n%d\n", primaryDSID, cloudName, versionNumber);
        String replicaDSIDString = replicaDSIDs.stream().map(i -> i.toString() + ";").collect(Collectors.joining());
        String replicaRatiosString = replicaRatios.stream().map(i -> i.toString() + ";").collect(Collectors.joining());
        stringSummary = primaryString + replicaDSIDString + "\n" + replicaRatiosString + "\n";
    }

    public ZKShardDescription(String stringSummary) {
        this.stringSummary = stringSummary;
        String[] ccv = stringSummary.split("\n");
        assert(ccv.length >= 3);
        primaryDSID = Integer.parseInt(ccv[0]);
        cloudName = ccv[1];
        versionNumber = Integer.parseInt(ccv[2]);
        if (ccv.length > 3) {
            replicaDSIDs = Arrays.stream(ccv[3].split(";")).map(Integer::parseInt).collect(Collectors.toList());
            replicaRatios = Arrays.stream(ccv[4].split(";")).map(Double::parseDouble).collect(Collectors.toList());
        } else {
            replicaDSIDs = Collections.emptyList();
            replicaRatios = Collections.emptyList();
        }
        assert(replicaDSIDs.size() == replicaRatios.size());
    }
}

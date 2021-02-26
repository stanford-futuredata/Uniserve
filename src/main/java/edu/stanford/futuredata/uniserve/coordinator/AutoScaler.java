package edu.stanford.futuredata.uniserve.coordinator;

import java.util.Map;

public interface AutoScaler {

    int NOCHANGE = 0;
    int ADD = 1;
    int REMOVE = 2;

    int autoscale(Map<Integer, Double> serverCpuUsage);
}

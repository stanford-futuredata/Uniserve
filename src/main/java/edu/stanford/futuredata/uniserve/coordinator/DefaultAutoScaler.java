package edu.stanford.futuredata.uniserve.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.OptionalDouble;

public class DefaultAutoScaler implements AutoScaler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAutoScaler.class);

    private int quiescence = 0;
    public final int quiescencePeriod = 2;
    public final double addServerThreshold = 0.7;
    public final double removeServerThreshold = 0.3;

    @Override
    public int autoscale(Map<Integer, Double> serverCpuUsage) {
        OptionalDouble averageCpuUsageOpt = serverCpuUsage.values().stream().mapToDouble(i -> i).average();
        if (averageCpuUsageOpt.isEmpty()) {
            return NOCHANGE;
        }
        double averageCpuUsage = averageCpuUsageOpt.getAsDouble();
        logger.info("Average CPU Usage: {}", averageCpuUsage);
        // After acting, wait quiescencePeriod cycles before acting again for CPU to rebalance.
        if (quiescence > 0) {
            quiescence--;
            return NOCHANGE;
        }
        // Add a server.
        if (averageCpuUsage > addServerThreshold) {
            quiescence = quiescencePeriod;
            return ADD;
        }
        // Remove a server.
        if (averageCpuUsage < removeServerThreshold) {
            quiescence = quiescencePeriod;
            return REMOVE;
        }
        return NOCHANGE;
    }
}

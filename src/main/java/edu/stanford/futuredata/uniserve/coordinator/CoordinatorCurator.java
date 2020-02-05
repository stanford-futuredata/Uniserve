package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

class CoordinatorCurator {
    // TODO:  Figure out what to actually do when ZK fails.
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorCurator.class);
    private final CuratorFramework cf;

    CoordinatorCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
    }

    void registerCoordinator(String host, int port) {
        // Create coordinator location node.
        try {
            String path = "/coordinator_host_port";
            byte[] data = String.format("%s:%d", host, port).getBytes();
            if (cf.checkExists().forPath(path) != null) {
                cf.setData().forPath(path, data);
            } else {
                cf.create().forPath(path, data);
            }

            // Create directory root nodes.
            String dsDescriptionPath = "/dsDescription";
            if (cf.checkExists().forPath(dsDescriptionPath) != null) {
                cf.setData().forPath(dsDescriptionPath, new byte[0]);
            } else {
                cf.create().forPath(dsDescriptionPath, new byte[0]);
            }
            String shardMappingPath = "/shardMapping";
            if (cf.checkExists().forPath(shardMappingPath) != null) {
                cf.setData().forPath(shardMappingPath, new byte[0]);
            } else {
                cf.create().forPath(shardMappingPath, new byte[0]);
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
        }
    }

    void setDSDescription(DataStoreDescription dsDescription) {
        try {
            String path = String.format("/dsDescription/%d", dsDescription.dsID);
            byte[] data = dsDescription.summaryString.getBytes();
            if (cf.checkExists().forPath(path) != null) {
                cf.setData().forPath(path, data);
            } else {
                cf.create().forPath(path, data);
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
        }
    }

    ZKShardDescription getZKShardDescription(int shard) {
        try {
            String path = String.format("/shardMapping/%d", shard);
            byte[] b = cf.getData().forPath(path);
            return new ZKShardDescription(new String(b));
        } catch (Exception e) {
            logger.error("getZKShardDescription Shard {} ZK Error: {}", shard, e.getMessage());
            assert(false);
            return null;
        }
    }

    void setZKShardDescription(int shard, int dsID, String cloudName, int versionNumber, List<Integer> replicaDSIDs, List<Double> replicaRatios) {
        try {
            String path = String.format("/shardMapping/%d", shard);
            ZKShardDescription zkShardDescription = new ZKShardDescription(dsID, cloudName, versionNumber, replicaDSIDs, replicaRatios);
            byte[] data = zkShardDescription.stringSummary.getBytes();
            if (cf.checkExists().forPath(path) != null) {
                cf.setData().forPath(path, data);
            } else {
                cf.create().forPath(path, data);
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
        }
    }

}

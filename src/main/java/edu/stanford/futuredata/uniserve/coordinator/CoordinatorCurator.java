package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            String shardReplicaMappingPath = "/shardReplicaMapping";
            if (cf.checkExists().forPath(shardReplicaMappingPath) != null) {
                cf.setData().forPath(shardReplicaMappingPath, new byte[0]);
            } else {
                cf.create().forPath(shardReplicaMappingPath, new byte[0]);
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
        }
    }

    void setDSDescription(int dsID, String host, int port) {
        try {
            String path = String.format("/dsDescription/%d", dsID);
            byte[] data = String.format("%s:%d", host, port).getBytes();
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

    void setZKShardDescription(int shard, int dsID, String cloudName, int versionNumber) {
        try {
            String path = String.format("/shardMapping/%d", shard);
            ZKShardDescription zkShardDescription = new ZKShardDescription(dsID, cloudName, versionNumber);
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

    void setShardReplicas(int shard, List<Integer> dsIDs) {
        try {
            String path = String.format("/shardReplicaMapping/%d", shard);
            StringBuilder replicaStringBuilder = new StringBuilder();
            for (Integer dsID : dsIDs) {
                replicaStringBuilder.append(dsID.toString()).append('\n');
            }
            byte[] data = replicaStringBuilder.toString().getBytes();
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

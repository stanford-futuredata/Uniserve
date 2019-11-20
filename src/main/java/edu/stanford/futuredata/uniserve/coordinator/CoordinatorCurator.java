package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.datastore.ZKShardDescription;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.List;

class CoordinatorCurator {

    private final CuratorFramework cf;

    CoordinatorCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
    }

    void registerCoordinator(String host, int port) throws Exception {
        // Create coordinator location node.
        String path = "/coordinator_host_port";
        byte[] data = String.format("%s:%d", host, port).getBytes();
        if (cf.checkExists().forPath(path) != null) {
            cf.setData().forPath(path, data);
        } else {
            cf.create().forPath(path, data);
        }

        // Create directory root nodes.
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
    }

    void setZKShardDescription(int shard, String primaryConnectString, String cloudName, int versionNumber) throws Exception {
        String path = String.format("/shardMapping/%d", shard);
        ZKShardDescription zkShardDescription = new ZKShardDescription(primaryConnectString, cloudName, versionNumber);
        byte[] data = zkShardDescription.stringSummary.getBytes();
        if (cf.checkExists().forPath(path) != null) {
            cf.setData().forPath(path, data);
        } else {
            cf.create().forPath(path, data);
        }
    }

    void setShardReplicas(int shard, List<String> replicaConnectStrings) throws Exception {
        String path = String.format("/shardReplicaMapping/%d", shard);
        StringBuilder replicaStringBuilder = new StringBuilder();
        for (String replicaConnectString: replicaConnectStrings) {
            replicaStringBuilder.append(replicaConnectString).append('\n');
        }
        byte[] data = replicaStringBuilder.toString().getBytes();
        if (cf.checkExists().forPath(path) != null) {
            cf.setData().forPath(path, data);
        } else {
            cf.create().forPath(path, data);
        }
    }

}

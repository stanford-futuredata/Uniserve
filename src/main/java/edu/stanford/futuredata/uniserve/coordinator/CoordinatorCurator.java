package edu.stanford.futuredata.uniserve.coordinator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

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
    }

    void setShardConnectString(int shard, String connectString) throws Exception {
        String path = String.format("/shardMapping/%d", shard);
        byte[] data = connectString.getBytes();
        if (cf.checkExists().forPath(path) != null) {
            cf.setData().forPath(path, data);
        } else {
            cf.create().forPath(path, data);
        }
    }

}

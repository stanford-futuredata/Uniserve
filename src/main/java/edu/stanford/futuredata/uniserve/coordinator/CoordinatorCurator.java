package edu.stanford.futuredata.uniserve.coordinator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CoordinatorCurator {

    private final CuratorFramework cf;

    public CoordinatorCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
    }

    public void registerCoordinator(String host, int port) throws Exception {
        String path = "/coordinator_host_port";
        byte[] data = String.format("%s:%d", host, port).getBytes();
        if (cf.checkExists().forPath(path) != null) {
            cf.setData().forPath(path, data);
        } else {
            cf.create().forPath(path, data);
        }
    }

}

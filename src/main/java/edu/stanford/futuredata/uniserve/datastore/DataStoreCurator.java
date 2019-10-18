package edu.stanford.futuredata.uniserve.datastore;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.Optional;

public class DataStoreCurator {

    private final CuratorFramework cf;

    public DataStoreCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
    }

    public Optional<Pair<String, Integer>> getMasterLocation() {
        try {
            String path = "/coordinator_host_port";
            byte[] b = cf.getData().forPath(path);
            String connectString = new String(b);
            String[] hostPort = connectString.split(":");
            String host = hostPort[0];
            Integer port = Integer.parseInt(hostPort[1]);
            return Optional.of(new Pair<>(host, port));
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }
}

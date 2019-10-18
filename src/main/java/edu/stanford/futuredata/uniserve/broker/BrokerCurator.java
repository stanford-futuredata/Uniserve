package edu.stanford.futuredata.uniserve.broker;

import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.javatuples.Pair;

import java.util.Optional;

public class BrokerCurator {
    private final CuratorFramework cf;

    BrokerCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
    }

    Optional<Pair<String, Integer>> getShardConnectString(int shard) {
        try {
            String path = String.format("/shardMapping/%d", shard);
            byte[] b = cf.getData().forPath(path);
            String connectString = new String(b);
            return Optional.of(Utilities.parseConnectString(connectString));
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    Optional<Pair<String, Integer>> getMasterLocation() {
        try {
            String path = "/coordinator_host_port";
            byte[] b = cf.getData().forPath(path);
            String connectString = new String(b);
            return Optional.of(Utilities.parseConnectString(connectString));
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

}



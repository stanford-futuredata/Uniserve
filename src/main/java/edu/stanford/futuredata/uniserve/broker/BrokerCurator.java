package edu.stanford.futuredata.uniserve.broker;

import edu.stanford.futuredata.uniserve.datastore.ZKShardDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.javatuples.Pair;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class BrokerCurator {
    private final CuratorFramework cf;

    BrokerCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
    }

    Optional<Pair<String, Integer>> getShardPrimaryConnectString(int shard) {
        try {
            String path = String.format("/shardMapping/%d", shard);
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                ZKShardDescription zkShardDescription = new ZKShardDescription(new String(b));
                String connectString = zkShardDescription.primaryConnectString;
                return Optional.of(Utilities.parseConnectString(connectString));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    Optional<Pair<Pair<String, Integer>, List<Pair<String, Integer>>>> getShardPrimaryReplicaConnectStrings(int shard) {
        try {
            String path = String.format("/shardMapping/%d", shard);
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                ZKShardDescription zkShardDescription = new ZKShardDescription(new String(b));
                Pair<String, Integer> primaryConnectString = Utilities.parseConnectString(zkShardDescription.primaryConnectString);
                List<Pair<String, Integer>> replicaConnectStrings =
                        zkShardDescription.secondaryConnectStrings.stream().map(Utilities::parseConnectString).collect(Collectors.toList());
                return Optional.of(new Pair<>(primaryConnectString, replicaConnectStrings));
            } else {
                return Optional.empty();
            }
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



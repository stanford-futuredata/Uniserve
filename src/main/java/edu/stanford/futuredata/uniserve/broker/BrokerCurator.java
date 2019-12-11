package edu.stanford.futuredata.uniserve.broker;

import edu.stanford.futuredata.uniserve.datastore.ZKShardDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class BrokerCurator {
    // TODO:  Figure out what to actually do when ZK fails.
    private final CuratorFramework cf;
    private static final Logger logger = LoggerFactory.getLogger(BrokerCurator.class);

    BrokerCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
    }

    String getConnectStringFromDSID(int dsID) {
        try {
            String path = String.format("/dsDescription/%d", dsID);
            byte[] b = cf.getData().forPath(path);
            return new String(b);
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }

    Optional<Pair<String, Integer>> getShardPrimaryConnectString(int shard) {
        try {
            String path = String.format("/shardMapping/%d", shard);
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                ZKShardDescription zkShardDescription = new ZKShardDescription(new String(b));
                String connectString = getConnectStringFromDSID(zkShardDescription.dsID);
                return Optional.of(Utilities.parseConnectString(connectString));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }

    Optional<List<Pair<String, Integer>>> getShardReplicaConnectStrings(int shard) {
        try {
            String path = String.format("/shardReplicaMapping/%d", shard);
            if (cf.checkExists().forPath(path) != null) {
                String replicaDataString = new String(cf.getData().forPath(path));
                if (replicaDataString.length() > 0) {
                    List<String> replicaStringDSIDs = Arrays.asList(replicaDataString.split("\n"));
                    List<Pair<String, Integer>> replicaHostPorts =
                            replicaStringDSIDs.stream().map(Integer::parseInt).map(this::getConnectStringFromDSID).map(Utilities::parseConnectString).collect(Collectors.toList());
                    return Optional.of(replicaHostPorts);
                } else {
                    return Optional.of(new ArrayList<>());
                }
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }

    Optional<Pair<String, Integer>> getMasterLocation() {
        try {
            String path = "/coordinator_host_port";
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                String connectString = new String(b);
                return Optional.of(Utilities.parseConnectString(connectString));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }

}



package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class DataStoreCurator {
    // TODO:  Figure out what to actually do when ZK fails.
    private final CuratorFramework cf;
    private static final Logger logger = LoggerFactory.getLogger(DataStoreCurator.class);

    DataStoreCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
    }

    void close() {
        cf.close();
    }

    DataStoreDescription getDSDescription(int dsID) {
        try {
            String path = String.format("/dsDescription/%d", dsID);
            byte[] b = cf.getData().forPath(path);
            return new DataStoreDescription(new String(b));
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }

    int getTransactionStatus(long txID) {
        try {
            String path = String.format("/txStatus/%d", txID);
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                return ByteBuffer.wrap(b).getInt();
            } else {
                return -1;
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            return -1;
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

    void setZKShardDescription(int shard, String cloudName, int versionNumber) {
        try {
            String path = String.format("/shardMapping/%d", shard);
            ZKShardDescription zkShardDescription = new ZKShardDescription(cloudName, versionNumber);
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

    ConsistentHash getConsistentHashFunction() {
        try {
            String path = "/consistentHash";
            byte[] b = cf.getData().forPath(path);
            return (ConsistentHash) Utilities.byteStringToObject(ByteString.copyFrom(b));
        } catch (Exception e) {
            logger.error("getConsistentHash Error: {}", e.getMessage());
            assert(false);
            return null;
        }
    }

    List<DataStoreDescription> getOtherDSDescriptions(int dsID) {
        int i = 0;
        List<DataStoreDescription> connectInfoList = new ArrayList<>();
        try {
            while(true) {
                String path = String.format("/dsDescription/%d", i);
                if (cf.checkExists().forPath(path) != null) {
                    DataStoreDescription dsDescription = getDSDescription(i);
                    if (i != dsID  && dsDescription.status.get() == DataStoreDescription.ALIVE) {
                        connectInfoList.add(dsDescription);
                    }
                } else {
                    break;
                }
                i++;
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
        return connectInfoList;
    }
}

package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Broker {

    private final QueryEngine queryEngine;
    private final BrokerCurator zkCurator;

    private static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    // Map from host/port pairs (used to uniquely identify a DataStore) to stubs.
    private final Map<Pair<String, Integer>, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> connStringToStubMap = new ConcurrentHashMap<>();
    // Map from shards to the primary's DataStoreBlockingStubs.
    private final Map<Integer, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> shardToPrimaryStubMap = new ConcurrentHashMap<>();
    // Map from shards to the replicas' DataStoreBlockingStubs.
    private final Map<Integer, List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub>> shardToReplicaStubMap = new ConcurrentHashMap<>();
    // Stub for communication with the coordinator.
    private BrokerCoordinatorGrpc.BrokerCoordinatorBlockingStub coordinatorBlockingStub;
    // Maximum number of shards.
    private static int numShards;
    // Daemon thread updating the shard maps.
    private ShardMapUpdateDaemon shardMapUpdateDaemon;
    // Should the daemon run?
    private boolean runShardMapUpdateDaemon = true;
    // How long should the daemon wait between runs?
    private static final int shardMapDaemonSleepDurationMillis = 1000;


    /*
     * CONSTRUCTOR/TEARDOWN
     */

    public Broker(String zkHost, int zkPort, QueryEngine queryEngine, int numShards) {
        this.queryEngine = queryEngine;
        Broker.numShards = numShards;
        this.zkCurator = new BrokerCurator(zkHost, zkPort);
        Optional<Pair<String, Integer>> masterHostPort = zkCurator.getMasterLocation();
        String masterHost = null;
        Integer masterPort = null;
        if (masterHostPort.isPresent()) {
            masterHost = masterHostPort.get().getValue0();
            masterPort = masterHostPort.get().getValue1();
        } else {
            logger.error("Broker could not find master"); // TODO:  Retry.
        }
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(masterHost, masterPort).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        coordinatorBlockingStub = BrokerCoordinatorGrpc.newBlockingStub(channel);
        shardMapUpdateDaemon = new ShardMapUpdateDaemon();
        shardMapUpdateDaemon.start();
    }

    public void shutdown() {
        runShardMapUpdateDaemon = false;
        try {
            shardMapUpdateDaemon.interrupt();
            shardMapUpdateDaemon.join();
        } catch (InterruptedException ignored) {}
        // TODO:  Synchronize with outstanding queries?
        ((ManagedChannel) this.coordinatorBlockingStub.getChannel()).shutdown();
        for (BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub: this.shardToPrimaryStubMap.values()) {
            ((ManagedChannel) stub.getChannel()).shutdown();
        }
        for (List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubs: this.shardToReplicaStubMap.values()) {
            for (BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub: stubs) {
                ((ManagedChannel) stub.getChannel()).shutdown();
            }
        }
    }

    /*
     * PUBLIC FUNCTIONS
     */

    public Integer insertRow(Row row) {
        int partitionKey = row.getPartitionKey();
        if (partitionKey < 0) {
            logger.warn("Negative partition key {}", partitionKey);
            return 1;
        }
        int shard = keyToShard(partitionKey);
        Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getPrimaryStubForShard(shard);
        if (stubOpt.isEmpty()) {
            logger.warn("Could not find DataStore for shard {}", shard);
            return 1;
        }
        BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = stubOpt.get();
        ByteString rowData;
        try {
            rowData = Utilities.objectToByteString(row);
        } catch (IOException e) {
            logger.warn("Row Serialization Failed: {}", e.getMessage());
            return 1;
        }
        InsertRowMessage rowMessage = InsertRowMessage.newBuilder().setShard(shard).setRowData(rowData).build();
        InsertRowResponse addRowAck;
        try {
            addRowAck = stub.insertRow(rowMessage);
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed: {}", e.getStatus());
            return 1;
        }
        return addRowAck.getReturnCode();
    }

    public <S extends Shard, T extends Serializable, V> Pair<Integer, V> readQuery(QueryPlan<S, T, V> queryPlan) {
        List<Integer> partitionKeys = queryPlan.keysForQuery();
        List<Integer> shards;
        if (partitionKeys.contains(-1)) {
            // -1 is a wildcard--run on all shards.
            shards = IntStream.range(0, numShards).boxed().collect(Collectors.toList());
        } else {
            shards = partitionKeys.stream().map(Broker::keyToShard).distinct().collect(Collectors.toList());
        }
        List<ReadQueryThread<T>> readQueryThreads = new ArrayList<>();
        for (int shard: shards) {
            ReadQueryThread<T> readQueryThread = new ReadQueryThread<T>(shard, queryPlan);
            readQueryThreads.add(readQueryThread);
            readQueryThread.start();
        }
        List<T> intermediates = new ArrayList<>();
        // TODO:  Query fault tolerance.
        for (ReadQueryThread<T> readQueryThread : readQueryThreads) {
            try {
                readQueryThread.join();
            } catch (InterruptedException e) {
                logger.warn("Interrupt: {}", e.getMessage());
                return new Pair<>(1, null);
            }
            Optional<T> intermediate = readQueryThread.getIntermediate();
            if (intermediate.isPresent()) {
                intermediates.add(intermediate.get());
            } else {
                logger.warn("Query Failure");
                return new Pair<>(1, null);
            }
        }
        V responseString = queryPlan.aggregateShardQueries(intermediates);
        return new Pair<>(0, responseString);
    }

    /*
     * PRIVATE FUNCTIONS
     */

    private static int keyToShard(int partitionKey) {
        return partitionKey % Broker.numShards;
    }

    private BrokerDataStoreGrpc.BrokerDataStoreBlockingStub getStubFromHostPort(Pair<String, Integer> hostPort) {
        BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = connStringToStubMap.getOrDefault(hostPort, null);
        if (stub == null) {
            ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(hostPort.getValue0(), hostPort.getValue1()).usePlaintext();
            ManagedChannel channel = channelBuilder.build();
            connStringToStubMap.putIfAbsent(hostPort, BrokerDataStoreGrpc.newBlockingStub(channel));
            stub = connStringToStubMap.get(hostPort);
        }
        return stub;
    }

    private Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> getPrimaryStubForShard(int shard) {
        // First, check the local shard-to-server map.
        BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = shardToPrimaryStubMap.getOrDefault(shard, null);
        if (stub == null) {
            // TODO:  This is thread-safe, but might make many redundant requests.
            Pair<String, Integer> hostPort;
            // Then, try to pull it from ZooKeeper.
            Optional<Pair<String, Integer>> hostPortOpt = zkCurator.getShardPrimaryConnectString(shard);
            if (hostPortOpt.isPresent()) {
                hostPort = hostPortOpt.get();
            } else {
                // Otherwise, ask the coordinator.
                ShardLocationMessage m = ShardLocationMessage.newBuilder().setShard(shard).build();
                ShardLocationResponse r;
                try {
                    r = coordinatorBlockingStub.shardLocation(m);
                } catch (StatusRuntimeException e) {
                    logger.warn("RPC failed: {}", e.getStatus());
                    return Optional.empty();
                }
                hostPort = Utilities.parseConnectString(r.getConnectString());
            }
            stub = getStubFromHostPort(hostPort);
            shardToPrimaryStubMap.putIfAbsent(shard, stub);
            stub = shardToPrimaryStubMap.get(shard);
        }
        return Optional.of(stub);
    }

    private Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> getAnyStubForShard(int shard) {
        // If replicas are known, return a random replica.
        List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubs = shardToReplicaStubMap.getOrDefault(shard, null);
        if (stubs != null) {
            return Optional.of(stubs.get(ThreadLocalRandom.current().nextInt(stubs.size())));
        }
        // Otherwise, return the primary if it is known.
        return getPrimaryStubForShard(shard);
    }

    private class ShardMapUpdateDaemon extends Thread {
        @Override
        public void run() {
            while (runShardMapUpdateDaemon) {
                for (Integer shardNum : shardToPrimaryStubMap.keySet()) {
                    Optional<Pair<Pair<String, Integer>, List<Pair<String, Integer>>>> connectStrings =
                            zkCurator.getShardPrimaryReplicaConnectStrings(shardNum);
                    if (connectStrings.isEmpty()) {
                        logger.error("ZK has lost information on Shard {}", shardNum);
                        continue;
                    }
                    Pair<String, Integer> primaryHostPort = connectStrings.get().getValue0();
                    BrokerDataStoreGrpc.BrokerDataStoreBlockingStub primaryStub = getStubFromHostPort(primaryHostPort);
                    List<Pair<String, Integer>> replicaHostPorts = connectStrings.get().getValue1();
                    List<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> replicaStubs = new ArrayList<>();
                    for (Pair<String, Integer> replicaHostPort: replicaHostPorts) {
                        replicaStubs.add(getStubFromHostPort(replicaHostPort));
                    }
                    shardToPrimaryStubMap.put(shardNum, primaryStub);
                    shardToReplicaStubMap.put(shardNum, replicaStubs);
                }
                try {
                    Thread.sleep(shardMapDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class ReadQueryThread<T extends Serializable> extends Thread {
        private final int shard;
        private final QueryPlan queryPlan;
        private Optional<T> intermediate;

        ReadQueryThread(int shard, QueryPlan queryPlan) {
            this.shard = shard;
            this.queryPlan = queryPlan;
        }

        @Override
        public void run() {
            this.intermediate = queryShard(this.shard);
        }

        private Optional<T> queryShard(int shard) {
            Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getAnyStubForShard(shard);
            if (stubOpt.isEmpty()) {
                logger.warn("Could not find DataStore for shard {}", shard);
                return Optional.empty();
            }
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = stubOpt.get();
            ByteString serializedQuery;
            try {
                serializedQuery = Utilities.objectToByteString(queryPlan);
            } catch (IOException e) {
                logger.warn("Query Serialization Failed: {}", e.getMessage());
                return Optional.empty();
            }
            ReadQueryMessage readQuery = ReadQueryMessage.newBuilder().setShard(shard).setSerializedQuery(serializedQuery).build();
            ReadQueryResponse readQueryResponse;
            try {
                readQueryResponse = stub.readQuery(readQuery);
                assert readQueryResponse.getReturnCode() == 0;
            } catch (StatusRuntimeException e) {
                logger.warn("RPC failed: {}", e.getStatus());
                return Optional.empty();
            }
            ByteString responseByteString = readQueryResponse.getResponse();
            T obj;
            try {
                obj = (T) Utilities.byteStringToObject(responseByteString);
            } catch (IOException | ClassNotFoundException e) {
                logger.warn("Deserialization failed: {}", e.getMessage());
                return Optional.empty();
            }
            return Optional.of(obj);
        }

        Optional<T> getIntermediate() {
            return this.intermediate;
        }
    }
}


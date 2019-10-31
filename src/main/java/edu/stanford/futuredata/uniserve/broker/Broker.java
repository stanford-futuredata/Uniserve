package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.stream.Collectors;

public class Broker {

    private final QueryEngine queryEngine;
    private final BrokerCurator zkCurator;

    private static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    // Map from host/port pairs (used to uniquely identify a DataStore) to stubs.
    private final Map<Pair<String, Integer>, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> connStringToStubMap = new HashMap<>();
    // Map from shards to DataStoreBlockingStubs.
    private final Map<Integer, BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> shardToStubMap = new HashMap<>();
    // Stub for communication with the coordinator.
    private BrokerCoordinatorGrpc.BrokerCoordinatorBlockingStub coordinatorBlockingStub;


    public Broker(String zkHost, int zkPort, QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
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
    }

    private Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> getStubForShard(int shard) {
        // TODO:  Synchronization.
        if (shardToStubMap.containsKey(shard)) {
            // First, see if we have the mapping locally.
            return Optional.of(shardToStubMap.get(shard));
        } else {
            Pair<String, Integer> hostPort;
            // Then, try to pull it from ZooKeeper.
            Optional<Pair<String, Integer>> hostPortOpt = zkCurator.getShardConnectString(shard);
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
            final BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub;
            if (connStringToStubMap.containsKey(hostPort)) {
                stub = connStringToStubMap.get(hostPort);
            } else {
                ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(hostPort.getValue0(), hostPort.getValue1()).usePlaintext();
                ManagedChannel channel = channelBuilder.build();
                stub = BrokerDataStoreGrpc.newBlockingStub(channel);
                connStringToStubMap.put(hostPort, stub);
            }
            shardToStubMap.put(shard, stub);
            return Optional.of(stub);
        }
    }


    public Integer insertRow(int partitionKey, ByteString rowData) {
        int shard = this.queryEngine.keyToShard(partitionKey);
        Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getStubForShard(shard);
        if (stubOpt.isEmpty()) {
            logger.warn("Could not find DataStore for shard {}", shard);
            return 1;
        }
        BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = stubOpt.get();
        InsertRowMessage row = InsertRowMessage.newBuilder().setShard(shard).setRowData(rowData).build();
        InsertRowResponse addRowAck;
        try {
            addRowAck = stub.insertRow(row);
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed: {}", e.getStatus());
            return 1;
        }
        return addRowAck.getReturnCode();
    }

    public Pair<Integer, String> readQuery(String query) {
        QueryPlan queryPlan = queryEngine.planQuery(query);
        List<Integer> partitionKeys = queryPlan.keysForQuery(); // TODO:  Slow for huge number of keys.
        List<Integer> shards = partitionKeys.stream().map(queryEngine::keyToShard).distinct().collect(Collectors.toList());
        List<ByteString> intermediates = new ArrayList<>();
        for (int shard: shards) { // TODO:  These should run in parallel.
            Optional<BrokerDataStoreGrpc.BrokerDataStoreBlockingStub> stubOpt = getStubForShard(shard);
            if (stubOpt.isEmpty()) {
                logger.warn("Could not find DataStore for shard {}", shard);
                return new Pair<>(1, "");
            }
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = stubOpt.get();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out;
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(queryPlan);
                out.flush();
            } catch (IOException e) {
                logger.warn("Query Serialization Failed: {}", e.getMessage());
                return new Pair<>(1, "");
            }
            ByteString serializedQuery = ByteString.copyFrom(bos.toByteArray());
            ReadQueryMessage readQuery = ReadQueryMessage.newBuilder().setShard(0).setSerializedQuery(serializedQuery).build();
            ReadQueryResponse readQueryResponse;
            try {
                readQueryResponse = stub.readQuery(readQuery);
                assert readQueryResponse.getReturnCode() == 0;
            } catch (StatusRuntimeException e) {
                logger.warn("RPC failed: {}", e.getStatus());
                return new Pair<>(1, "");
            }
            intermediates.add(readQueryResponse.getResponse());
        }
        String responseString = queryPlan.aggregateShardQueries(intermediates);
        return new Pair<>(0, responseString);
    }
}


package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class ServiceBrokerCoordinator extends BrokerCoordinatorGrpc.BrokerCoordinatorImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerCoordinator.class);

    private final Coordinator coordinator;

    ServiceBrokerCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void shardLocation(ShardLocationMessage request,
                              StreamObserver<ShardLocationResponse> responseObserver) {
        responseObserver.onNext(shardLocationHandler(request));
        responseObserver.onCompleted();
    }

    private ShardLocationResponse shardLocationHandler(ShardLocationMessage m) {
        int shardNum = m.getShard();
        coordinator.shardMapLock.lock();
        // Check if the shard's location is known.
        Integer dsID = coordinator.shardToPrimaryDataStoreMap.getOrDefault(shardNum, null);
        if (dsID != null) {
            DataStoreDescription dsDesc = coordinator.dataStoresMap.get(dsID);
            coordinator.shardMapLock.unlock();
            return ShardLocationResponse.newBuilder().setReturnCode(0).setDsID(dsID).setHost(dsDesc.host).setPort(dsDesc.port).build();
        }
        // If not, assign it to a DataStore.
        dsID = coordinator.assignShardToDataStore(shardNum);
        coordinator.shardToPrimaryDataStoreMap.put(shardNum, dsID);
        coordinator.shardToReplicaDataStoreMap.put(shardNum, new ArrayList<>());
        coordinator.shardToReplicaRatioMap.put(shardNum, new ArrayList<>());
        coordinator.shardMapLock.unlock();
        // Tell the DataStore to create the shard.
        DataStoreDescription dsDesc = coordinator.dataStoresMap.get(dsID);
        CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = coordinator.dataStoreStubsMap.get(dsID);
        CreateNewShardMessage cns = CreateNewShardMessage.newBuilder().setShard(shardNum).build();
        try {
            CreateNewShardResponse cnsResponse = stub.createNewShard(cns);
            assert cnsResponse.getReturnCode() == 0;
        } catch (StatusRuntimeException e) {
            logger.warn("Shard {} create RPC failed on DataStore {}", shardNum, dsID);
            assert(false);
        }
        // Once the shard is created, add it to the ZooKeeper map.
        coordinator.shardMapLock.lock();
        int primaryDataStore = coordinator.shardToPrimaryDataStoreMap.get(shardNum);
        List<Integer> replicaDataStores = coordinator.shardToReplicaDataStoreMap.get(shardNum);
        List<Double> replicaRatios = coordinator.shardToReplicaRatioMap.get(shardNum);
        coordinator.zkCurator.setZKShardDescription(shardNum, primaryDataStore, Utilities.null_name, 0, replicaDataStores, replicaRatios);
        coordinator.shardMapLock.unlock();
        return ShardLocationResponse.newBuilder().setReturnCode(0).setDsID(dsID).setHost(dsDesc.host).setPort(dsDesc.port).build();
    }

    @Override
    public void queryStatistics(QueryStatisticsMessage request, StreamObserver<QueryStatisticsResponse> responseObserver) {
        responseObserver.onNext(queryStatisticsHandler(request));
        responseObserver.onCompleted();
    }

    private QueryStatisticsResponse queryStatisticsHandler(QueryStatisticsMessage m) {
        ConcurrentHashMap<Set<Integer>, Integer> queryStatistics = (ConcurrentHashMap<Set<Integer>, Integer>) Utilities.byteStringToObject(m.getQueryStatistics());
        coordinator.statisticsLock.lock();
        queryStatistics.forEach((s, v) -> coordinator.queryStatistics.merge(s, v, Integer::sum));
        coordinator.statisticsLock.unlock();
        return QueryStatisticsResponse.newBuilder().build();
    }

    @Override
    public void createTable(CreateTableMessage request, StreamObserver<CreateTableResponse> responseObserver) {
        responseObserver.onNext(createTableHandler(request));
        responseObserver.onCompleted();
    }

    private CreateTableResponse createTableHandler(CreateTableMessage m) {
        String tableName = m.getTableName();
        int numShards = m.getNumShards();
        if (coordinator.tableNumShardsMap.putIfAbsent(tableName, numShards) != null) {
            return CreateTableResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
        } else {
            int tableID = coordinator.tableNumber.getAndIncrement();
            Integer n = coordinator.tableIDMap.putIfAbsent(tableName, tableID);
            assert (n == null);
            logger.info("Creating Table. Name: {} ID: {} NumShards {}", tableName, tableID, numShards);
            return CreateTableResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).build();
        }
    }

    @Override
    public void tableID(TableIDMessage request, StreamObserver<TableIDResponse> responseObserver) {
        responseObserver.onNext(tableIDHandler(request));
        responseObserver.onCompleted();
    }

    private TableIDResponse tableIDHandler(TableIDMessage m) {
        String tableName = m.getTableName();
        if (coordinator.tableIDMap.containsKey(tableName)) {
            int tableID = coordinator.tableIDMap.get(tableName);
            int numShards = coordinator.tableNumShardsMap.get(tableName);
            return TableIDResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS)
                    .setId(tableID)
                    .setNumShards(numShards).build();
        } else {
            return TableIDResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
        }
    }
}

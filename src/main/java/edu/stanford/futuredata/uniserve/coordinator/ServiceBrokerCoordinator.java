package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
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

    private int assignShardToDataStore(int shardNum) {
        // TODO:  Better DataStore choice.
        return shardNum % coordinator.dataStoresMap.size();
    }

    private ShardLocationResponse shardLocationHandler(ShardLocationMessage m) {
        int shardNum = m.getShard();
        // Check if the shard's location is known.
        Integer dsID = coordinator.shardToPrimaryDataStoreMap.getOrDefault(shardNum, null);
        if (dsID != null) {
            DataStoreDescription dsDesc = coordinator.dataStoresMap.get(dsID);
            return ShardLocationResponse.newBuilder().setReturnCode(0).setDsID(dsID).setHost(dsDesc.host).setPort(dsDesc.port).build();
        }
        // If not, assign it to a DataStore.
        coordinator.shardMapLock.lock();
        int newDSID = assignShardToDataStore(shardNum);
        dsID = coordinator.shardToPrimaryDataStoreMap.putIfAbsent(shardNum, newDSID);
        if (dsID != null) {
            DataStoreDescription dsDesc = coordinator.dataStoresMap.get(dsID);
            coordinator.shardMapLock.unlock();
            return ShardLocationResponse.newBuilder().setReturnCode(0).setDsID(dsID).setHost(dsDesc.host).setPort(dsDesc.port).build();
        }
        dsID = newDSID;
        coordinator.shardToReplicaDataStoreMap.put(shardNum, new ArrayList<>());
        coordinator.shardToReplicaRatioMap.put(shardNum, new ArrayList<>());
        coordinator.shardToVersionMap.put(shardNum, 0);
        // Tell the DataStore to create the shard.
        DataStoreDescription dsDesc = coordinator.dataStoresMap.get(dsID);
        CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = coordinator.dataStoreStubsMap.get(dsID);
        CreateNewShardMessage cns = CreateNewShardMessage.newBuilder().setShard(shardNum).build();
        CreateNewShardResponse cnsResponse = stub.createNewShard(cns);
        assert cnsResponse.getReturnCode() == 0; //TODO:  Error handling.
        // Once the shard is created, add it to the ZooKeeper map.
        coordinator.zkCurator.setZKShardDescription(m.getShard(), dsID, Utilities.null_name, 0, Collections.emptyList(), Collections.emptyList());
        coordinator.shardMapLock.unlock();
        return ShardLocationResponse.newBuilder().setReturnCode(0).setDsID(dsID).setHost(dsDesc.host).setPort(dsDesc.port).build();
    }

    @Override
    public void shardAffinity(ShardAffinityMessage request, StreamObserver<ShardAffinityResponse> responseObserver) {
        responseObserver.onNext(shardAffinityHandler(request));
        responseObserver.onCompleted();
    }

    private ShardAffinityResponse shardAffinityHandler(ShardAffinityMessage m) {
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Integer>> affinityCounts = (ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Integer>>) Utilities.byteStringToObject(m.getAffinityCounts());
        ConcurrentHashMap<Integer, Integer> readQueryCounts = (ConcurrentHashMap<Integer, Integer>) Utilities.byteStringToObject(m.getReadQueryCounts());
        coordinator.affinityLock.lock();
        readQueryCounts.forEach((r, v) -> coordinator.readQueryCounts.merge(r, v, Integer::sum));
        affinityCounts.keySet().forEach(k -> coordinator.affinityCounts.putIfAbsent(k, new ConcurrentHashMap<>()));
        affinityCounts.forEach((r, map) -> map.forEach((s, count) -> coordinator.affinityCounts.get(r).merge(s, count, Integer::sum)));
        coordinator.affinityLock.unlock();
        return ShardAffinityResponse.newBuilder().build();
    }
}

package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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
            String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
            return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
        }
        // If not, assign it to a DataStore.
        int newDSID = assignShardToDataStore(shardNum);
        dsID = coordinator.shardToPrimaryDataStoreMap.putIfAbsent(shardNum, newDSID);
        if (dsID != null) {
            DataStoreDescription dsDesc = coordinator.dataStoresMap.get(dsID);
            String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
            return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
        }
        coordinator.shardToReplicaDataStoreMap.put(shardNum, new ArrayList<>());
        coordinator.shardToVersionMap.put(shardNum, 0);
        // Tell the DataStore to create the shard.
        DataStoreDescription dsDesc = coordinator.dataStoresMap.get(newDSID);
        CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dsDesc.stub;
        CreateNewShardMessage cns = CreateNewShardMessage.newBuilder().setShard(shardNum).build();
        CreateNewShardResponse cnsResponse = stub.createNewShard(cns);
        assert cnsResponse.getReturnCode() == 0; //TODO:  Error handling.
        // Once the shard is created, add it to the ZooKeeper map.
        coordinator.zkCurator.setZKShardDescription(m.getShard(), newDSID, Utilities.null_name, 0);
        coordinator.zkCurator.setShardReplicas(m.getShard(), new ArrayList<>());
        String connectString = String.format("%s:%d", dsDesc.host, dsDesc.port);
        return ShardLocationResponse.newBuilder().setReturnCode(0).setConnectString(connectString).build();
    }

}

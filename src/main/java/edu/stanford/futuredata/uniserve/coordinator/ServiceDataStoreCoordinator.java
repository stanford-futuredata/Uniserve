package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

class ServiceDataStoreCoordinator extends DataStoreCoordinatorGrpc.DataStoreCoordinatorImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceDataStoreCoordinator.class);

    private final Coordinator coordinator;

    ServiceDataStoreCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void registerDataStore(RegisterDataStoreMessage request,
                                  StreamObserver<RegisterDataStoreResponse> responseObserver) {
        responseObserver.onNext(registerDataStoreHandler(request));
        responseObserver.onCompleted();
    }

    private RegisterDataStoreResponse registerDataStoreHandler(RegisterDataStoreMessage m) {
        String host = m.getHost();
        int port = m.getPort();
        int dsID = coordinator.dataStoreNumber.getAndIncrement();
        DataStoreDescription dsDescription = new DataStoreDescription(dsID, DataStoreDescription.ALIVE, host, port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        coordinator.dataStoreChannelsMap.put(dsID, channel);
        CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = CoordinatorDataStoreGrpc.newBlockingStub(channel);
        coordinator.dataStoreStubsMap.put(dsID, stub);
        coordinator.zkCurator.setDSDescription(dsDescription);
        coordinator.dataStoresMap.put(dsID, dsDescription);
        logger.info("Registered DataStore ID: {} Host: {} Port: {}", dsID, host, port);
        return RegisterDataStoreResponse.newBuilder().setReturnCode(0).setDataStoreID(dsID).build();
    }

    @Override
    public void shardUpdate(ShardUpdateMessage request, StreamObserver<ShardUpdateResponse> responseObserver) {
        responseObserver.onNext(shardUpdateHandler(request));
        responseObserver.onCompleted();
    }

    private ShardUpdateResponse shardUpdateHandler(ShardUpdateMessage m) {
        int shardNum = m.getShardNum();
        String cloudName = m.getShardCloudName();
        int versionNumber = m.getVersionNumber();
        coordinator.shardMapLock.lock();
        int dsID = coordinator.shardToPrimaryDataStoreMap.get(shardNum);
        List<Integer> replicaDSIDs = coordinator.shardToReplicaDataStoreMap.get(shardNum);
        List<Double> replicaRatios = coordinator.shardToReplicaRatioMap.get(shardNum);
        coordinator.shardToVersionMap.put(shardNum, versionNumber);
        coordinator.zkCurator.setZKShardDescription(shardNum, dsID, cloudName, versionNumber, replicaDSIDs, replicaRatios);
        coordinator.shardMapLock.unlock();
        return ShardUpdateResponse.newBuilder().setReturnCode(0).build();
    }

    @Override
    public void potentialDSFailure(PotentialDSFailureMessage request, StreamObserver<PotentialDSFailureResponse> responseObserver) {
        responseObserver.onNext(potentialDSFailureHandler(request));
        responseObserver.onCompleted();
    }

    private PotentialDSFailureResponse potentialDSFailureHandler(PotentialDSFailureMessage request) {
        int dsID = request.getDsID();
        CoordinatorPingMessage m = CoordinatorPingMessage.newBuilder().build();
        try {
            CoordinatorPingResponse alwaysEmpty = coordinator.dataStoreStubsMap.get(dsID).coordinatorPing(m);
        } catch (StatusRuntimeException e) {
            DataStoreDescription dsDescription = coordinator.dataStoresMap.get(dsID);
            if (dsDescription.status.compareAndSet(DataStoreDescription.ALIVE, DataStoreDescription.DEAD)) {
                logger.warn("DS{} Failure Detected", dsID);
                coordinator.zkCurator.setDSDescription(dsDescription);
            }
        }
        return PotentialDSFailureResponse.newBuilder().build();
    }
}

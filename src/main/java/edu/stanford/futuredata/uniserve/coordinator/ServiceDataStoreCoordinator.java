package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        coordinator.dataStoresMap.put(dsID, new DataStoreDescription(host, port));
        coordinator.zkCurator.setDSDescription(dsID, host, port);
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
        int dsID = coordinator.shardToPrimaryDataStoreMap.get(shardNum);
        coordinator.shardToVersionMap.put(shardNum, versionNumber);
        coordinator.zkCurator.setZKShardDescription(shardNum, dsID, cloudName, versionNumber);
        return ShardUpdateResponse.newBuilder().setReturnCode(0).build();
    }

    @Override
    public void potentialDSFailure(PotentialDSFailureMessage request, StreamObserver<PotentialDSFailureResponse> responseObserver) {
        responseObserver.onNext(potentialDSFailureHandler(request));
        responseObserver.onCompleted();
    }

    private PotentialDSFailureResponse potentialDSFailureHandler(PotentialDSFailureMessage request) {
        int dsID = request.getDsID();
        DataStoreDescription dsDescription = coordinator.dataStoresMap.get(dsID);
        CoordinatorPingMessage m = CoordinatorPingMessage.newBuilder().build();
        try {
            CoordinatorPingResponse alwaysEmpty = dsDescription.stub.coordinatorPing(m);
        } catch (StatusRuntimeException e) {
            logger.warn("DS{} Failure Detected", dsID);
        }
        return PotentialDSFailureResponse.newBuilder().build();
    }
}

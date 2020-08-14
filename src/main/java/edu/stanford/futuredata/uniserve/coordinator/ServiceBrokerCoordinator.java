package edu.stanford.futuredata.uniserve.coordinator;

import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class ServiceBrokerCoordinator extends BrokerCoordinatorGrpc.BrokerCoordinatorImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerCoordinator.class);

    private final Coordinator coordinator;

    ServiceBrokerCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
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
        int tableID = coordinator.tableNumber.getAndIncrement();
        TableInfo t = new TableInfo(tableName, tableID, numShards);
        if (coordinator.tableInfoMap.putIfAbsent(tableName, t) != null) {
            return CreateTableResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
        } else {
            logger.info("Creating Table. Name: {} ID: {} NumShards {}", tableName, tableID, numShards);
            return CreateTableResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).build();
        }
    }

    @Override
    public void tableInfo(TableInfoMessage request, StreamObserver<TableInfoResponse> responseObserver) {
        responseObserver.onNext(tableIDHandler(request));
        responseObserver.onCompleted();
    }

    private TableInfoResponse tableIDHandler(TableInfoMessage m) {
        String tableName = m.getTableName();
        if (coordinator.tableInfoMap.containsKey(tableName)) {
            TableInfo t = coordinator.tableInfoMap.get(tableName);
            return TableInfoResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS)
                    .setId(t.id)
                    .setNumShards(t.numShards).build();
        } else {
            return TableInfoResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
        }
    }
}

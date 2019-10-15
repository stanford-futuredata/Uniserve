package edu.stanford.futuredata.uniserve.datastore;

import edu.stanford.futuredata.uniserve.QueryDataGrpc;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class DataStore {

    private final Server server;

    public DataStore(int port, ShardFactory shardFactory) {
        ServerBuilder serverBuilder = ServerBuilder.forPort(port);
        this.server = serverBuilder.addService(new QueryDataService(shardFactory))
                .build();
    }

    public void startServing() {

    }

    private static class QueryDataService extends QueryDataGrpc.QueryDataImplBase {

        private final ShardFactory shardFactory;

        QueryDataService(ShardFactory shardFactory) {
            this.shardFactory = shardFactory;
        }

    }
}

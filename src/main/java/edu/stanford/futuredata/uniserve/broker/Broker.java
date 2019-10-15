package edu.stanford.futuredata.uniserve.broker;

import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;

public class Broker {

    private final QueryEngine queryEngine;

    public Broker(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }
}


package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;

public class KVQueryEngine implements QueryEngine {

    @Override
    public QueryPlan planQuery(String query) {
        Integer key = Integer.parseInt(query);
        return new KVQueryPlan(key);
    }
}

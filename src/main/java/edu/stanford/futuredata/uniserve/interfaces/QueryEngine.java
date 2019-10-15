package edu.stanford.futuredata.uniserve.interfaces;

import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;

public interface QueryEngine {
    /*
     Lives on a query server.  Generates a query plan (query functions and target keys) for any user-submitted query.
     */

    // Generate a query plan for a user-submitted query.
    public QueryPlan planQuery(String query);
}

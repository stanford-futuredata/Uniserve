package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class KVWriteQueryPlanInsertSlow implements WriteQueryPlan<KVRow, KVShard> {

    @Override
    public boolean preCommit(KVShard shard, List<KVRow> rows) {
        shard.setRows(rows);
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(80, 120));
        } catch (InterruptedException ignored) { }
        return true;
    }

    @Override
    public void commit(KVShard shard) {

        try {
            shard.insertRows();
        } catch (NullPointerException ignored) {
        }
    }

    @Override
    public void abort(KVShard shard) {

    }
}

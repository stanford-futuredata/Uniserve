package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaterializedView {
    // TODO:  Define a cache eviction policy.
    private final Map<Long, ByteString> cachedIntermediates = new HashMap<>();
    private final ReadQueryPlan r;
    private final Shard s;

    public MaterializedView(ReadQueryPlan r, Shard s, long firstTimeStamp, ByteString firstIntermediate) {
        this.r = r;
        this.s = s;
        cachedIntermediates.put(firstTimeStamp, firstIntermediate);
    }

    public ByteString getLatestView() {
        long mostRecentTimestamp = cachedIntermediates.keySet().stream().mapToLong(i -> i).max().getAsLong();
        return cachedIntermediates.get(mostRecentTimestamp);
    }

    public void updateView(long writeStartStamp, long lastExistingStamp) {
        long mostRecentUsableTimestamp = cachedIntermediates.keySet().stream().mapToLong(i -> i).filter(i -> i < writeStartStamp).max().orElse(-1);
        ByteString newIntermediate;
        if (mostRecentUsableTimestamp == -1) {
            newIntermediate = r.queryShard(s);
        } else {
            ByteString oldIntermediate = cachedIntermediates.get(mostRecentUsableTimestamp);
            ByteString incrementalUpdate = r.queryShard(s, mostRecentUsableTimestamp, lastExistingStamp);
            newIntermediate = r.combineIntermediates(List.of(oldIntermediate, incrementalUpdate));
        }
        cachedIntermediates.put(lastExistingStamp, newIntermediate);
    }
}

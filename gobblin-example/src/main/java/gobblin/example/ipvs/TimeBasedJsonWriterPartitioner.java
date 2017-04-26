package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

/**
 * Created by JYan on 4/26/17.
 */
public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<JsonElement> {


    public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
    }

    @Override
    public long getRecordTimestamp(JsonElement record) {
        return record.getAsJsonObject().get("time").getAsLong();
    }
}

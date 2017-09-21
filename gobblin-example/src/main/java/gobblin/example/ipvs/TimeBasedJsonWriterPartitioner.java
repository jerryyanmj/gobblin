package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

/**
 * Created by JYan on 4/26/17.
 */
public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<JsonElement> {

    protected static final String TIME_BASED_JSON_WRITER_PARTITION_KEY = "time.based.json.writer.partition.key";
    private final Logger log = LoggerFactory.getLogger(TimeBasedJsonWriterPartitioner.class);
    protected String partitionKey;

    public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
        partitionKey = state.getProperties().getProperty(TIME_BASED_JSON_WRITER_PARTITION_KEY, "time");
    }

    @Override
    public long getRecordTimestamp(JsonElement record) {
        JsonElement tsElem = record.getAsJsonObject().get(partitionKey);
        if (tsElem == null)
            return Calendar.getInstance().getTimeInMillis();
        return tsElem.getAsLong();
    }
}

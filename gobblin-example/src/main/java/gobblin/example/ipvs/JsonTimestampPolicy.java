package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonTimestampPolicy extends RowLevelPolicy{

    private static final Logger logger = LoggerFactory.getLogger(JsonTimestampPolicy.class);

    protected String partitionKey;

    public JsonTimestampPolicy(State state, Type type) {
        super(state, type);
        partitionKey = state.getProperties().getProperty(TimeBasedJsonWriterPartitioner.TIME_BASED_JSON_WRITER_PARTITION_KEY, "time");
        logger.info("Time based json key {}", partitionKey);
    }

    @Override
    public Result executePolicy(Object record) {

        JsonElement tsElem = ((JsonElement)record).getAsJsonObject().get(partitionKey);
        if (tsElem != null) {
            return RowLevelPolicy.Result.PASSED;
        }

        logger.warn("Json Timestamp Checker Policy violation: {}", record.toString());

        return RowLevelPolicy.Result.FAILED;
    }
}

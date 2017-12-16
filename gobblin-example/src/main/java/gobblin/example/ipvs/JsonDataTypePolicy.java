package gobblin.example.ipvs;

import com.google.gson.JsonObject;
import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonDataTypePolicy extends RowLevelPolicy {

    private static final Logger logger = LoggerFactory.getLogger(JsonDataTypePolicy.class);

    public JsonDataTypePolicy(State state, Type type) {
        super(state, type);
    }

    @Override
    public Result executePolicy(Object record) {

        if (record instanceof JsonObject) {
            JsonObject recordObject = (JsonObject) record;
            if (recordObject.entrySet().size() != 0)
                return RowLevelPolicy.Result.PASSED;
        }

        logger.warn("Json Type Checker Policy violation: {}", record.toString());
        return RowLevelPolicy.Result.FAILED;
    }
}

package gobblin.example.ipvs;

import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;
import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

/**
 * Created by JYan on 4/26/17.
 */
public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<JsonElement> {

    private String timestampField;

    public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);

        String topicName = state.getProp("topic.name");
        JsonArray topicSpecificProps = state.getPropAsJsonArray("kafka.topic.specific.state");
        for (JsonElement prop : topicSpecificProps) {
          String propTopicName = prop.getAsJsonObject().get("topic.name").getAsString();
          String propColumnName = prop.getAsJsonObject().get("writer.partition.columns").getAsString();
          if (propTopicName.equals(topicName)) {
            this.timestampField = propColumnName;
          }
        }
    }

    @Override
    public long getRecordTimestamp(JsonElement record) {
        JsonObject jsonObject = record.getAsJsonObject();
        JsonElement timestamp = jsonObject.get(this.timestampField);

        if (timestamp == null) {
          return System.currentTimeMillis();
        } else {
          return timestamp.getAsLong();
        }
    }
}

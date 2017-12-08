package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by JYan on 4/26/17.
 */
public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<JsonElement> {

    protected static final String TIME_BASED_JSON_WRITER_PARTITION_KEY = "time.based.json.writer.partition.key";
    protected static final String TIME_BASED_JSON_WRITER_PARTITION_KEY_DATE_FORMAT = "time.based.json.writer.partition.key.date.format";
    private final Logger log = LoggerFactory.getLogger(TimeBasedJsonWriterPartitioner.class);
    protected String partitionKey;
    protected String partitionKeyDateFormatStr;

    public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
        partitionKey = state.getProperties().getProperty(TIME_BASED_JSON_WRITER_PARTITION_KEY, "time");
        partitionKeyDateFormatStr = state.getProperties().getProperty(TIME_BASED_JSON_WRITER_PARTITION_KEY_DATE_FORMAT,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    }

    @Override
    public long getRecordTimestamp(JsonElement record) {

        try {

            JsonElement tsElem = record.getAsJsonObject().get(partitionKey);

            if (StringUtils.isNumeric(tsElem.getAsString()))
                return tsElem.getAsLong();

            if (!tsElem.isJsonNull()) {
                SimpleDateFormat sdf = new SimpleDateFormat(partitionKeyDateFormatStr);
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                Calendar c = Calendar.getInstance();

                try {
                    Date d = sdf.parse(tsElem.getAsString());
                    c.setTime(d);
                    c.setTimeZone(TimeZone.getTimeZone("UTC"));
                    return c.getTimeInMillis();
                } catch (ParseException ex) {
                    return Calendar.getInstance().getTimeInMillis();
                }
            }

        } catch (RuntimeException ex) {
            return Calendar.getInstance().getTimeInMillis();
        }

        return Calendar.getInstance().getTimeInMillis();
    }
}

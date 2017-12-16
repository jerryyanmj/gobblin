package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Stream;

import java.io.*;
import java.util.Arrays;

/**
 * Created by JYan on 4/24/17.
 */
public class KafkaJsonExtractor extends KafkaExtractor<String, JsonElement> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaJsonExtractor.class);

    public KafkaJsonExtractor(WorkUnitState state) {

        super(state);

        logger.debug("KafkaJsonExtractor is instantiated.");
    }

    @Override
    public String getSchema() throws IOException {
        return null;
    }

    @Override
    protected JsonElement decodeRecord(MessageAndOffset messageAndOffset) throws IOException {

        byte[] payload = getBytes(messageAndOffset.message().payload());

        InputStream is = new ByteArrayInputStream(payload);

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        try {
            JsonElement element = GSON.fromJson(reader, JsonElement.class);
            return element;
        } catch (Exception ex ) {
            logger.warn("Payload cannot converted to JSON object. {}", new String(payload));
        }

        return new JsonObject();
    }
}
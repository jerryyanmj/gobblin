package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

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

        JsonElement element = GSON.fromJson(reader, JsonElement.class);

        logger.debug(element.toString());

        return element;
    }
}
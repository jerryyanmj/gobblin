package gobblin.example.twc;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaAvroExtractor;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An abstract implementation of {@link Extractor} for Kafka, where events are in Avro format.
 *
 */
public class TWCConopsKafkaAvroExtractor extends KafkaAvroExtractor<String> {

    public static final String SCHEMA_REPO_SCHEMA_EXTENSION_KEY = "schema.extension";

    protected static final String DEFAULT_SCHEMA_EXTENSION = "avro";

    private static final Logger log = LoggerFactory.getLogger(TWCConopsKafkaAvroExtractor.class);

    protected Map<String, Schema> schemaCache;
    protected Map<String, String> schemaRepoUrlMap;
    protected String schemaRepoUrl;
    protected String schemaExt;

    public TWCConopsKafkaAvroExtractor(WorkUnitState state) {
        super(state);
        log.info("Load TWCConopsKafkaAvroExtractor Properties.");
        this.schemaRepoUrlMap = new ConcurrentHashMap<String, String>();
        this.schemaCache = new ConcurrentHashMap<String, Schema>();

        String str = state.getProperties().getProperty(TWCConopsKafkaSchemaRegistry.SCHEMA_REPO_BASE_URL_KEY, StringUtils.EMPTY);
        if (StringUtils.isNotEmpty(str)) {
            String[] pairs = StringUtils.split(str, ";");
            for (String pair : pairs) {
                String[] kv = StringUtils.split(pair, "|");
                if (ArrayUtils.getLength(kv) == 2) {
                    this.schemaRepoUrlMap.put(kv[0], kv[1]);
                }
            }
        }

        this.schemaRepoUrl = schemaRepoUrlMap.getOrDefault(this.topicName, TWCConopsKafkaSchemaRegistry.DEFAULT_SCHEMA_REPO_BASE_URL);
        this.schemaExt = state.getProperties().getProperty(SCHEMA_REPO_SCHEMA_EXTENSION_KEY, DEFAULT_SCHEMA_EXTENSION);
    }

    @Override
    protected Schema getRecordSchema(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        String schemaId = String.valueOf(buffer.getInt());

        // Try to get the schema from the concurrent map
        Schema recordSchema = schemaCache.get(schemaId);

        if (recordSchema == null) {
            recordSchema = TWCConopsKafkaSchemaUtil.getSchemaByURL(this.schemaRepoUrl + "schema_" + schemaId + "." + schemaExt);
            schemaCache.put(schemaId, recordSchema);
        }

        return recordSchema;
    }

    @Override
    protected Decoder getDecoder(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        buffer.getInt();
        return DecoderFactory.get().binaryDecoder(buffer.array(), buffer.position() + buffer.arrayOffset(), buffer.limit(), null);
    }
}

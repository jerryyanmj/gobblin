package gobblin.example.twc;

import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.avro.Schema;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiarui.yan on 2016-06-10.
 */
public class TWCConopsKafkaSchemaRegistry extends KafkaSchemaRegistry<String, Schema> {

    protected static final String SCHEMA_REPO_BASE_URL_KEY = "schema.repo.base.url";
    protected static final String DEFAULT_SCHEMA_REPO_BASE_URL = "http://haproxy.escher-twc.com:83/EG/";

    private static final Logger log = LoggerFactory.getLogger(TWCConopsKafkaSchemaRegistry.class);

    protected Map<String, String> schemaRepoUrl;

    public TWCConopsKafkaSchemaRegistry(Properties props) {

        super(props);

        this.schemaRepoUrl = new ConcurrentHashMap<String, String>();

        String str = props.getProperty(SCHEMA_REPO_BASE_URL_KEY, StringUtils.EMPTY);
        log.info("Loading schema repo URLs from configuration: {}", str);
        if (StringUtils.isNotEmpty(str)) {
            String[] pairs = StringUtils.split(str, ";");
            for (String pair : pairs) {
                String[] kv = StringUtils.split(pair, "|");
                if (ArrayUtils.getLength(kv) == 2) {
                    this.schemaRepoUrl.put(kv[0], kv[1]);
                }
            }
        }
        log.info("Loaded schema repo URLs.", this.schemaRepoUrl);
    }

    @Override
    protected Schema fetchSchemaByKey(String key) throws SchemaRegistryException {
        log.info("Fetch schema for topic {}.", key);
        String url = this.schemaRepoUrl.get(key) + "current";
        Schema schema = TWCConopsKafkaSchemaUtil.getSchemaByURL(url);
        register(schema, key);
        return schema;
    }

    @Override
    public Schema getLatestSchemaByTopic(String topic) throws SchemaRegistryException {
        log.info("Get latest schema for topic {}.", topic);
        Schema schema = cachedSchemasByKeys.getIfPresent(topic);
        if (schema == null) {
            schema = fetchSchemaByKey(topic);
        }
        return schema;
    }

    @Override
    public String register(Schema schema) throws SchemaRegistryException {
        return null;
    }

    @Override
    public String register(Schema schema, String name) throws SchemaRegistryException {
        log.info("Register schema for topic {}.", name);
        cachedSchemasByKeys.put(name, schema);
        return null;
    }

}

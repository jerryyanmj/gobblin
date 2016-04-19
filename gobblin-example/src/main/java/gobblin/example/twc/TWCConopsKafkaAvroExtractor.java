package gobblin.example.twc;

import com.google.common.base.Optional;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaAvroExtractor;
import kafka.message.MessageAndOffset;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An abstract implementation of {@link Extractor} for Kafka, where events are in Avro format.
 *
 */
public class TWCConopsKafkaAvroExtractor extends KafkaAvroExtractor<String> {

    private final static String SCHEMA_REPO_BASE_URL_KEY = "schema.repo.base.url";
    protected final static String DEFAULT_SCHEMA_REPO_BASE_URL = "http://haproxy.escher-twc.com:83/EG/";
    private final static String SCHEMA_REPO_SCHEMA_EXTENSION_KEY = "schema.extension";
    protected final static String DEFAULT_SCHEMA_EXTENSION = "avro";
    private final Logger log = LoggerFactory.getLogger(TWCConopsKafkaAvroExtractor.class);

    protected Map<String, Schema> schemaCache;
    protected String schemaRepoUrl;
    protected String schemaExt;
    protected String defaultClassPathSchema;


    public TWCConopsKafkaAvroExtractor(WorkUnitState state) {
        super(state);
        log.info("Load TWCConopsKafkaAvroExtractor Properties.");
        this.schemaRepoUrl = state.getProperties().getProperty(SCHEMA_REPO_BASE_URL_KEY, DEFAULT_SCHEMA_REPO_BASE_URL);
        this.schemaExt = state.getProperties().getProperty(SCHEMA_REPO_SCHEMA_EXTENSION_KEY, DEFAULT_SCHEMA_EXTENSION);
        this.schemaCache = new ConcurrentHashMap<String, Schema>();
    }

    @Override
    protected Optional<Schema> getExtractorSchema() {
        InputStream defaultSchema = this.getClass().getClassLoader().getResourceAsStream("default.avsc");
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(defaultSchema);
        } catch (IOException e) {
            log.error("Invalid schema file");
        }
        return Optional.fromNullable(schema);
    }

    @Override
    protected GenericRecord decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
        byte[] payload = getBytes(messageAndOffset.message().payload());
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        String schemaId = String.valueOf(buffer.getInt());

        // Try to get the schema from the concurrent map
        Schema recordSchema = schemaCache.get(schemaId);

        if (recordSchema == null) {
            recordSchema = getSchemaByIdFromRepo(schemaId);
            schemaCache.put(schemaId, recordSchema);
        }


        Decoder decoder = DecoderFactory.get().binaryDecoder(buffer.array(), buffer.position() + buffer.arrayOffset(), buffer.limit(), null);
        this.reader.get().setExpected(recordSchema);
        this.reader.get().setSchema(recordSchema);
        try {
            GenericRecord record = this.reader.get().read(null, decoder);
            return record;
        } catch (IOException e) {
            log.error(String.format("Error during decoding record for partition %s: ", this.getCurrentPartition()));
            throw e;
        }
    }

    @Override
    protected Schema getRecordSchema(byte[] payload) { return null;}

    @Override
    protected Decoder getDecoder(byte[] payload) { return null; }

    private Schema getSchemaByIdFromRepo(String schemaId) {
        String schemaRepoURL = schemaRepoUrl + "schema_" + schemaId + "." + schemaExt;

        log.info("Schema is empty, getting it from repo using URL - {}", schemaRepoURL);

        try {
            HostnameVerifier hv = new HostnameVerifier() {
                public boolean verify(String urlHostName, SSLSession session) {
                    log.warn("Warning: URL Host: {} vs. {}", urlHostName, session.getPeerHost());
                    return true;
                }
            };

            trustAllHttpsCertificates();
            HttpsURLConnection.setDefaultHostnameVerifier(hv);

            Schema schema = new Schema.Parser().parse(new URL(schemaRepoURL).openStream());
            return schema;
        } catch (Exception ex) {
            log.error("Failed to obtain schema from the URL {}", schemaRepoURL);
        }

        return null;
    }

    private static void trustAllHttpsCertificates() throws Exception {
        javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[1];
        javax.net.ssl.TrustManager tm = new miTM();
        trustAllCerts[0] = tm;
        javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext
                .getInstance("SSL");
        sc.init(null, trustAllCerts, null);
        javax.net.ssl.HttpsURLConnection.setDefaultSSLSocketFactory(sc
                .getSocketFactory());
    }

    static class miTM implements javax.net.ssl.TrustManager, javax.net.ssl.X509TrustManager {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        public boolean isServerTrusted(
                java.security.cert.X509Certificate[] certs) {
            return true;
        }

        public boolean isClientTrusted(
                java.security.cert.X509Certificate[] certs) {
            return true;
        }

        public void checkServerTrusted(
                java.security.cert.X509Certificate[] certs, String authType)
                throws java.security.cert.CertificateException {
            return;
        }

        public void checkClientTrusted(
                java.security.cert.X509Certificate[] certs, String authType)
                throws java.security.cert.CertificateException {
            return;
        }
    }
}

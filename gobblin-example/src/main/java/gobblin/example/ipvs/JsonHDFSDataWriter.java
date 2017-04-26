package gobblin.example.ipvs;

import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;
import gobblin.configuration.State;
import gobblin.writer.FsDataWriter;
import gobblin.writer.FsDataWriterBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by JYan on 4/25/17.
 */
public class JsonHDFSDataWriter extends FsDataWriter<JsonElement> {

    private final DataFileWriter<JsonElement> writer;

    private final OutputStream stagingFileOutputStream;

    protected final AtomicLong count = new AtomicLong(0);
    protected final AtomicLong bytes = new AtomicLong(0);

    public JsonHDFSDataWriter(FsDataWriterBuilder<?, JsonElement> builder, State properties) throws IOException {
        super(builder, properties);

        this.writer = this.closer.register(createDataFileWriter(codecFactory));
    }

    @Override
    public void write(JsonElement record) throws IOException {

        Preconditions.checkNotNull(record);

        this.writer.append(record);
        // Only increment when write is successful
        this.count.incrementAndGet();

    }

    @Override
    public long recordsWritten() {
        return this.count.get();
    }

    @Override
    public long bytesWritten() throws IOException {
        if (!this.fs.exists(this.outputFile)) {
            return 0;
        }

        return this.fs.getFileStatus(this.outputFile).getLen();
    }

    private DataFileWriter<JsonElement> createDataFileWriter(CodecFactory codecFactory) throws IOException {
        @SuppressWarnings("resource")
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(this.datumWriter);
        writer.setCodec(codecFactory);

        // Open the file and return the DataFileWriter
        return writer.create(this.schema, this.stagingFileOutputStream);
    }
}

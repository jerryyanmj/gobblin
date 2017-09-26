package gobblin.example.ipvs;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.FsDataWriter;
import gobblin.writer.FsDataWriterBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Created by JYan on 4/25/17.
 */
public class JsonHDFSDataWriter extends FsDataWriter<JsonElement> {

    private final OutputStream stagingFileOutputStream;
    private final Optional<Byte> recordDelimiter;

    private int recordsWritten;
    private int bytesWritten;

    public JsonHDFSDataWriter(FsDataWriterBuilder<String, JsonElement> builder, State properties) throws IOException {
        super(builder, properties);

        this.recordDelimiter = Optional.of("\n".getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)[0]);

        this.recordsWritten = 0;
        this.bytesWritten = 0;
        this.stagingFileOutputStream = createStagingFileOutputStream();
    }

    @Override
    public void write(JsonElement record) throws IOException {

        Preconditions.checkNotNull(record);

        byte[] recordBytes = record.toString().getBytes();
        byte[] toWrite = recordBytes;

        if (this.recordDelimiter.isPresent()) {
            toWrite = Arrays.copyOf(recordBytes, recordBytes.length + 1);
            toWrite[toWrite.length - 1] = this.recordDelimiter.get();
        }

        this.stagingFileOutputStream.write(toWrite);
        this.bytesWritten += toWrite.length;
        this.recordsWritten++;

    }

    @Override
    public long recordsWritten() {
        return this.recordsWritten;
    }

    @Override
    public long bytesWritten() throws IOException {
        return this.bytesWritten;
    }

}

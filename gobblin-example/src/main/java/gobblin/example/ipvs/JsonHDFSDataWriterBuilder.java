package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import gobblin.writer.DataWriter;
import gobblin.writer.FsDataWriterBuilder;

import java.io.IOException;

/**
 * Created by JYan on 4/26/17.
 */
public class JsonHDFSDataWriterBuilder extends FsDataWriterBuilder<String, JsonElement> {
    @Override
    public DataWriter<JsonElement> build() throws IOException {
        return new JsonHDFSDataWriter(this, this.destination.getProperties());
    }
}

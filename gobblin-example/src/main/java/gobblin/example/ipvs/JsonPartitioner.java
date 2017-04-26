package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import gobblin.writer.partitioner.WriterPartitioner;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Created by JYan on 4/26/17.
 */
public class JsonPartitioner implements WriterPartitioner<JsonElement> {
    @Override
    public Schema partitionSchema() {
        return null;
    }

    @Override
    public GenericRecord partitionForRecord(JsonElement record) {
        return null;
    }
}

package gobblin.example.ipvs;

import com.google.gson.JsonElement;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSource;

import java.io.IOException;

/**
 * Created by JYan on 4/24/17.
 */
public class KafkaJsonSource extends KafkaSource<String, JsonElement> {
    @Override
    public Extractor<String, JsonElement> getExtractor(WorkUnitState state) throws IOException {
        return new KafkaJsonExtractor(state);
    }
}

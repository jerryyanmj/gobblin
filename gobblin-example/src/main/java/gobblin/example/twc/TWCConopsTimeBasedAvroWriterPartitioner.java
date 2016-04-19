package gobblin.example.twc;

import gobblin.configuration.State;
import gobblin.writer.partitioner.WriterPartitioner;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by jiarui.yan on 2016-03-03.
 */
enum EgwPartition {
    YEAR("year"), MONTH("month"), DAYOFMONTH("day_of_month"), HOUR("hour"), MINUTE("minute");
    public String name;
    EgwPartition(String name) {
        this.name = name;
    }
}

enum EgwPartitionSchema {
    YEAR(EgwPartition.YEAR.name,SchemaBuilder.record("egw").namespace("gobblin.example.twc")
            .fields()
            .name(EgwPartition.YEAR.name).type(Schema.create(Schema.Type.STRING)).noDefault().endRecord()),
    MONTH(EgwPartition.MONTH.name, SchemaBuilder.record("egw").namespace("gobblin.example.twc")
            .fields()
            .name(EgwPartition.YEAR.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.MONTH.name).type(Schema.create(Schema.Type.STRING)).noDefault().endRecord()),
    DAYOFMONTH(EgwPartition.DAYOFMONTH.name, SchemaBuilder.record("egw").namespace("gobblin.example.twc")
            .fields()
            .name(EgwPartition.YEAR.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.MONTH.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.DAYOFMONTH.name).type(Schema.create(Schema.Type.STRING)).noDefault().endRecord()),
    HOUR(EgwPartition.HOUR.name, SchemaBuilder.record("egw").namespace("gobblin.example.twc")
            .fields()
            .name(EgwPartition.YEAR.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.MONTH.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.DAYOFMONTH.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.HOUR.name).type(Schema.create(Schema.Type.STRING)).noDefault().endRecord()),
    MINUTE(EgwPartition.MINUTE.name, SchemaBuilder.record("egw").namespace("gobblin.example.twc")
            .fields()
            .name(EgwPartition.YEAR.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.MONTH.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.DAYOFMONTH.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.HOUR.name).type(Schema.create(Schema.Type.STRING)).noDefault()
            .name(EgwPartition.MINUTE.name).type(Schema.create(Schema.Type.STRING)).noDefault().endRecord());

    public String name;
    public Schema partitionSchema;

    EgwPartitionSchema(String name, Schema partitionSchema) {
        this.name = name;
        this.partitionSchema = partitionSchema;
    }


}

public class TWCConopsTimeBasedAvroWriterPartitioner implements WriterPartitioner<GenericRecord> {

    private final static String TWC_BASED_AVRO_WRITER_PARTITION_GRANULARITY_KEY = "time.based.avro.writer.partition.granularity";
    private final static String TWC_BASED_AVRO_WRITER_DEFAULT_PARTITION_GRANULARITY = "HOUR";
    private final static String TWC_BASED_AVRO_WRITER_PARTITION_GRANULARITY_STEP_KEY = "time.based.avro.writer.partition.granularity.step";
    private final static String TWC_BASED_AVRO_WRITER_DEFAULT_PARTITION_GRANULARITY_STEP = "3600";
    private final static String TWC_BASED_AVRO_WRITER_TIMESTAMP_FIELD_KEY = "time.based.avro.writer.timestamp.field";
    private final static String TWC_BASED_AVRO_WRITER_DEFAULT_TIMESTAMP_FIELD = "timestamp";

    private final Logger log = LoggerFactory.getLogger(TWCConopsTimeBasedAvroWriterPartitioner.class);

    private static EgwPartitionSchema granularity;
    private static Long granularityStep;
    private static String timestampField;


    private static final String UTC_TZ = "UTC";

    public TWCConopsTimeBasedAvroWriterPartitioner() {}

    public TWCConopsTimeBasedAvroWriterPartitioner(State state, int numBranches, int branchId) {
        this.granularity = EgwPartitionSchema.valueOf(state.getProperties().getProperty(TWC_BASED_AVRO_WRITER_PARTITION_GRANULARITY_KEY, TWC_BASED_AVRO_WRITER_DEFAULT_PARTITION_GRANULARITY));
        log.debug("Granularity is {} and partition schema is {}", this.granularity.name, this.granularity.partitionSchema);
        this.granularityStep = Long.parseLong(state.getProperties().getProperty(TWC_BASED_AVRO_WRITER_PARTITION_GRANULARITY_STEP_KEY, TWC_BASED_AVRO_WRITER_DEFAULT_PARTITION_GRANULARITY_STEP));
        this.timestampField = state.getProperties().getProperty(TWC_BASED_AVRO_WRITER_TIMESTAMP_FIELD_KEY, TWC_BASED_AVRO_WRITER_DEFAULT_TIMESTAMP_FIELD);
    }

    @Override
    public Schema partitionSchema() {
        return this.granularity.partitionSchema;
    }

    @Override
    public GenericRecord partitionForRecord(GenericRecord record) {
        GenericRecord partition = new GenericData.Record(this.granularity.partitionSchema);
        String ts = String.valueOf(record.get(this.timestampField));
        log.trace("timestamp field {} for record {} is {}", this.timestampField, record.toString(), ts);
        Long tsLong = Long.parseLong(ts);

        Long step = this.granularityStep;
        if (StringUtils.length(ts) != 10) {
            step = step * 1000;
        }

        Long roundedTsLong = Math.round(Math.floor(tsLong * 1D / step)) * step;

        TimeZone tz = TimeZone.getTimeZone(UTC_TZ);
        Calendar c = Calendar.getInstance(tz);
        c.setTimeInMillis(roundedTsLong);

        partition.put(EgwPartitionSchema.YEAR.name, String.valueOf(c.get(Calendar.YEAR)));
        partition.put(EgwPartitionSchema.MONTH.name, StringUtils.leftPad(String.valueOf(c.get(Calendar.MONTH) + 1), 2, '0'));
        partition.put(EgwPartitionSchema.DAYOFMONTH.name, StringUtils.leftPad(String.valueOf(c.get(Calendar.DAY_OF_MONTH)), 2, '0'));
        partition.put(EgwPartitionSchema.HOUR.name, StringUtils.leftPad(String.valueOf(c.get(Calendar.HOUR_OF_DAY)), 2, '0'));

        if (this.granularity == EgwPartitionSchema.MINUTE) {
            partition.put(EgwPartitionSchema.MINUTE.name, StringUtils.leftPad(String.valueOf(c.get(Calendar.MINUTE)), 2, '0'));
        }

        log.debug("Partition for {} : {}", tsLong, partition);

        return partition;
    }
}

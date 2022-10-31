package io.github.comrada.kafka.connect.http.record;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.connect.data.SchemaBuilder.int64;
import static org.apache.kafka.connect.data.SchemaBuilder.string;

import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import io.github.comrada.kafka.connect.http.record.spi.KvSourceRecordMapper;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class SchemedKvSourceRecordMapper implements KvSourceRecordMapper {

  private static final String KEY_FIELD_NAME = "key";
  private static final String VALUE_FIELD_NAME = "value";
  private static final String TIMESTAMP_FIELD_NAME = "timestamp";

  private static final Map<String, ?> SOURCE_PARTITION = emptyMap();

  private final Function<Map<String, ?>, SourceRecordMapperConfig> configFactory;

  private SourceRecordMapperConfig config;

  private Schema keySchema;

  private Schema valueSchema;

  SchemedKvSourceRecordMapper(Function<Map<String, ?>, SourceRecordMapperConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public SchemedKvSourceRecordMapper() {
    this(SourceRecordMapperConfig::new);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    config = configFactory.apply(settings);
    keySchema = SchemaBuilder.struct()
        .name("io.github.comrada.kafka.connect.http.Key").doc("Message Key")
        .field(KEY_FIELD_NAME, string().optional().doc("HTTP Record Key").build())
        .build();
    valueSchema = SchemaBuilder.struct()
        .name("io.github.comrada.kafka.connect.http.Value").doc("Message Value")
        .field(VALUE_FIELD_NAME, string().doc("HTTP Record Value").build())
        .field(KEY_FIELD_NAME, string().optional().doc("HTTP Record Key").build())
        .field(TIMESTAMP_FIELD_NAME, int64().optional().doc("HTTP Record Timestamp").build())
        .build();
  }

  @Override
  public SourceRecord map(KvRecord record) {

    Offset offset = record.getOffset();
    Long timestamp = offset.getTimestamp().map(Instant::toEpochMilli).orElseGet(System::currentTimeMillis);

    Struct key = keyStruct(record.getKey());
    Struct value = valueStruct(record.getKey(), record.getValue(), timestamp);

    return new SourceRecord(
        SOURCE_PARTITION,
        offset.toMap(),
        config.getTopic(),
        null,
        key.schema(),
        key,
        value.schema(),
        value,
        timestamp);
  }

  private Struct keyStruct(String key) {
    return new Struct(keySchema).put(KEY_FIELD_NAME, key);
  }

  private Struct valueStruct(String key, String value, Long timestamp) {
    return new Struct(valueSchema)
        .put(KEY_FIELD_NAME, key)
        .put(VALUE_FIELD_NAME, value)
        .put(TIMESTAMP_FIELD_NAME, timestamp);
  }
}

package io.github.comrada.kafka.connect.http.record;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.connect.data.SchemaBuilder.string;

import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import io.github.comrada.kafka.connect.http.record.spi.KvSourceRecordMapper;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @deprecated The same can be achieved with Kafka Connect's SMT ExtractKey/ExtractValue. e.g.
 * <p>
 * "transforms": "ExtractKey,ExtractValue", "transforms.ExtractKey.type":
 * "org.apache.kafka.connect.transforms.ExtractField$Key", "transforms.ExtractKey.field": "key",
 * "transforms.ExtractValue.type": "org.apache.kafka.connect.transforms.ExtractField$Value", "transforms.ExtractValue.field":
 * "value"
 */
@Deprecated
public class StringKvSourceRecordMapper implements KvSourceRecordMapper {

  private static final Map<String, ?> SOURCE_PARTITION = emptyMap();

  private static final Schema keySchema = string().build();

  private static final Schema valueSchema = string().build();

  private final Function<Map<String, ?>, SourceRecordMapperConfig> configFactory;

  private SourceRecordMapperConfig config;

  StringKvSourceRecordMapper(Function<Map<String, ?>, SourceRecordMapperConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public StringKvSourceRecordMapper() {
    this(SourceRecordMapperConfig::new);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    config = configFactory.apply(settings);
  }

  @Override
  public SourceRecord map(KvRecord record) {
    Offset offset = record.getOffset();
    return new SourceRecord(
        SOURCE_PARTITION,
        offset.toMap(),
        config.getTopic(),
        null,
        keySchema,
        record.getKey(),
        valueSchema,
        record.getValue(),
        offset.getTimestamp().map(Instant::toEpochMilli).orElseGet(System::currentTimeMillis));
  }
}

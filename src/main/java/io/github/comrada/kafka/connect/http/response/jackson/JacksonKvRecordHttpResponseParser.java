package io.github.comrada.kafka.connect.http.response.jackson;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.nameUUIDFromBytes;
import static java.util.stream.Collectors.toList;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import io.github.comrada.kafka.connect.http.response.jackson.model.JacksonRecord;
import io.github.comrada.kafka.connect.http.response.spi.KvRecordHttpResponseParser;
import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class JacksonKvRecordHttpResponseParser implements KvRecordHttpResponseParser {

  private final Function<Map<String, ?>, JacksonKvRecordHttpResponseParserConfig> configFactory;

  private JacksonResponseRecordParser responseParser;

  private TimestampParser timestampParser;

  JacksonKvRecordHttpResponseParser(Function<Map<String, ?>, JacksonKvRecordHttpResponseParserConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public JacksonKvRecordHttpResponseParser() {
    this(JacksonKvRecordHttpResponseParserConfig::new);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    JacksonKvRecordHttpResponseParserConfig config = configFactory.apply(configs);
    responseParser = config.getResponseParser();
    timestampParser = config.getTimestampParser();
  }

  @Override
  public List<KvRecord> parse(HttpResponse response) {
    return responseParser.getRecords(response.getBody())
        .map(this::map)
        .collect(toList());
  }

  private KvRecord map(JacksonRecord record) {
    final Map<String, Object> offsets = record.getOffset();
    final String key = Optional.ofNullable(record.getKey())
        .or(() -> Optional.ofNullable(offsets.get("key")).map(String.class::cast))
        .orElseGet(() -> generateConsistentKey(record.getBody()));
    final Optional<Instant> timestamp = Optional.ofNullable(record.getTimestamp())
        .or(() -> Optional.ofNullable(offsets.get("timestamp")).map(String.class::cast))
        .map(timestampParser::parse);
    final Offset offset = timestamp
        .map(ts -> Offset.of(offsets, key, ts))
        .orElseGet(() -> Offset.of(offsets, key));
    return KvRecord.builder()
        .key(key)
        .value(record.getBody())
        .offset(offset)
        .build();
  }

  private String generateConsistentKey(String body) {
    return nameUUIDFromBytes(body.getBytes()).toString();
  }
}

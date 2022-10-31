package io.github.comrada.kafka.connect.http.response.timestamp;

import static java.util.Objects.requireNonNull;

import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Function;

public class DateTimeFormatterTimestampParser implements TimestampParser {

  private final Function<Map<String, ?>, DateTimeFormatterTimestampParserConfig> configFactory;

  private DateTimeFormatter timestampFormatter;

  DateTimeFormatterTimestampParser(Function<Map<String, ?>, DateTimeFormatterTimestampParserConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public DateTimeFormatterTimestampParser() {
    this(DateTimeFormatterTimestampParserConfig::new);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    timestampFormatter = configFactory.apply(settings).getRecordTimestampFormatter();
  }

  @Override
  public Instant parse(String timestamp) {
    return OffsetDateTime.parse(timestamp, timestampFormatter).toInstant();
  }
}

package io.github.comrada.kafka.connect.http.response.timestamp;

import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;

public class EpochMillisOrDelegateTimestampParser implements TimestampParser {

  private final Function<Map<String, ?>, EpochMillisOrDelegateTimestampParserConfig> configFactory;

  private TimestampParser delegate;

  EpochMillisOrDelegateTimestampParser(Function<Map<String, ?>, EpochMillisOrDelegateTimestampParserConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public EpochMillisOrDelegateTimestampParser() {
    this(EpochMillisOrDelegateTimestampParserConfig::new);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    EpochMillisOrDelegateTimestampParserConfig config = configFactory.apply(settings);
    delegate = config.getDelegateParser();
  }

  @Override
  public Instant parse(String timestamp) {
    try {
      return Instant.ofEpochMilli(parseLong(timestamp));
    } catch (NumberFormatException e) {
      return delegate.parse(timestamp);
    }
  }
}

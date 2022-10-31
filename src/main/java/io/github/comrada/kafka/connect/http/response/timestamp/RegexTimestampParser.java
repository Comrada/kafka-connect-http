package io.github.comrada.kafka.connect.http.response.timestamp;

import static java.util.Objects.requireNonNull;

import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RegexTimestampParser implements TimestampParser {

  private final Function<Map<String, ?>, RegexTimestampParserConfig> configFactory;
  private Pattern pattern;
  private TimestampParser delegate;

  RegexTimestampParser(Function<Map<String, ?>, RegexTimestampParserConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public RegexTimestampParser() {
    this(RegexTimestampParserConfig::new);
  }

  @Override
  public Instant parse(String timestamp) {
    Matcher matcher = pattern.matcher(timestamp);
    String extractedTimestamp;
    matcher.find();
    extractedTimestamp = matcher.group(1);
    return delegate.parse(extractedTimestamp);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    RegexTimestampParserConfig config = configFactory.apply(settings);
    pattern = Pattern.compile(config.getTimestampRegex());
    delegate = config.getDelegateParser();
  }
}

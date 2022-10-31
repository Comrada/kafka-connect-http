package io.github.comrada.kafka.connect.http.response.timestamp;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;

import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class EpochMillisOrDelegateTimestampParserConfig extends AbstractConfig {

  private static final String PARSER_DELEGATE = "http.response.record.timestamp.parser.delegate";

  private final TimestampParser delegateParser;

  public EpochMillisOrDelegateTimestampParserConfig(Map<String, ?> originals) {
    super(config(), originals);
    delegateParser = getConfiguredInstance(PARSER_DELEGATE, TimestampParser.class);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(PARSER_DELEGATE, CLASS, DateTimeFormatterTimestampParser.class, HIGH, "Timestamp Parser Delegate Class");
  }
}

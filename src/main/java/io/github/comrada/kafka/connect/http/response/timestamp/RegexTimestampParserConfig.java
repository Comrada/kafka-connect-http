package io.github.comrada.kafka.connect.http.response.timestamp;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class RegexTimestampParserConfig extends AbstractConfig {

  private static final String ITEM_TIMESTAMP_REGEX = "http.response.record.timestamp.parser.regex";
  private static final String PARSER_DELEGATE = "http.response.record.timestamp.parser.regex.delegate";

  private final String timestampRegex;
  private final TimestampParser delegateParser;

  RegexTimestampParserConfig(Map<String, ?> originals) {
    super(config(), originals);
    timestampRegex = getString(ITEM_TIMESTAMP_REGEX);
    delegateParser = getConfiguredInstance(PARSER_DELEGATE, TimestampParser.class);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(ITEM_TIMESTAMP_REGEX, STRING, ".*", LOW,
            "Timestamp regex pattern in case the timestamp value is wrapped around some other text which is not in well defined format")
        .define(PARSER_DELEGATE, CLASS, DateTimeFormatterTimestampParser.class, HIGH, "Timestamp Parser Delegate Class");
  }
}

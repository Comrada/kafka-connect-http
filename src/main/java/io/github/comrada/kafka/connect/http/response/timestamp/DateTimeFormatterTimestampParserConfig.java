package io.github.comrada.kafka.connect.http.response.timestamp;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class DateTimeFormatterTimestampParserConfig extends AbstractConfig {

  private static final String ITEM_TIMESTAMP_PATTERN = "http.response.record.timestamp.parser.pattern";
  private static final String ITEM_TIMESTAMP_ZONE = "http.response.record.timestamp.parser.zone";

  private final DateTimeFormatter recordTimestampFormatter;

  DateTimeFormatterTimestampParserConfig(Map<String, ?> originals) {
    super(config(), originals);
    recordTimestampFormatter = DateTimeFormatter.ofPattern(getString(ITEM_TIMESTAMP_PATTERN))
        .withZone(ZoneId.of(getString(ITEM_TIMESTAMP_ZONE)));
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(ITEM_TIMESTAMP_PATTERN, STRING, "yyyy-MM-dd'T'HH:mm:ss[.SSS]X", LOW, "Timestamp format pattern")
        .define(ITEM_TIMESTAMP_ZONE, STRING, "UTC", LOW, "Timestamp ZoneId");
  }
}

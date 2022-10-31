package io.github.comrada.kafka.connect.http.response.timestamp;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class NattyTimestampParserConfig extends AbstractConfig {

  private static final String ITEM_TIMESTAMP_ZONE = "http.response.record.timestamp.parser.zone";

  private final Optional<ZoneId> timestampZoneId;

  NattyTimestampParserConfig(Map<String, ?> originals) {
    super(config(), originals);
    timestampZoneId = Optional.ofNullable(getString(ITEM_TIMESTAMP_ZONE)).map(ZoneId::of);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(ITEM_TIMESTAMP_ZONE, STRING, null, LOW,
            "Timestamp ZoneId, to be used only when timestamp doesn't include a timezone/offset reference");
  }
}

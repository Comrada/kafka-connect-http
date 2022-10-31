package io.github.comrada.kafka.connect.http.response.timestamp;

import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.time.Instant;

public class EpochMillisTimestampParser implements TimestampParser {

  @Override
  public Instant parse(String timestamp) {
    return Instant.ofEpochMilli(Long.parseLong(timestamp));
  }
}

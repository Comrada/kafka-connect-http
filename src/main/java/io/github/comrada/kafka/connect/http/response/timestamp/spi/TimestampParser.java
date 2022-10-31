package io.github.comrada.kafka.connect.http.response.timestamp.spi;

import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@FunctionalInterface
public interface TimestampParser extends Configurable {

  Instant parse(String timestamp);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

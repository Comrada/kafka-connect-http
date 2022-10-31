package io.github.comrada.kafka.connect.timer.spi;

import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@FunctionalInterface
public interface Timer extends Configurable {

  Long getRemainingMillis();

  default void reset(Instant lastZero) {
    // Do nothing
  }

  @Override
  default void configure(Map<String, ?> configs) {
    // Do nothing
  }
}

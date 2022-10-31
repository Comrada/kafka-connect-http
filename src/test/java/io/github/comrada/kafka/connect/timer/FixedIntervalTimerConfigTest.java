package io.github.comrada.kafka.connect.timer;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class FixedIntervalTimerConfigTest {

  @Test
  void whenPollIntervalMillis_thenDefault() {
    assertThat(new FixedIntervalTimerConfig(emptyMap()).getPollIntervalMillis()).isEqualTo(60000L);
  }

  @Test
  void whenPollIntervalMillis_thenInitialized() {
    assertThat(
        new FixedIntervalTimerConfig(ImmutableMap.of("http.timer.interval.millis", "42")).getPollIntervalMillis()).isEqualTo(
        42L);
  }

  @Test
  void whenDeprecatedPollIntervalMillis_thenInitialized() {
    assertThat(new FixedIntervalTimerConfig(
        ImmutableMap.of("http.throttler.interval.millis", "42")).getPollIntervalMillis()).isEqualTo(42L);
  }
}

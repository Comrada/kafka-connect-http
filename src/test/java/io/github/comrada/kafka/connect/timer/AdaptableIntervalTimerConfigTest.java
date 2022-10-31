package io.github.comrada.kafka.connect.timer;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AdaptableIntervalTimerConfigTest {

  @Test
  void whenTailIntervalMillis_thenDefault() {
    assertThat(config(emptyMap()).getTailTimer().getIntervalMillis()).isEqualTo(60000L);
  }

  @Test
  void whenTailIntervalMillis_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.timer.interval.millis", "42")).getTailTimer().getIntervalMillis()).isEqualTo(
        42L);
  }

  @Test
  void whenDeprecatedTailIntervalMillis_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.throttler.interval.millis", "42")).getTailTimer().getIntervalMillis()).isEqualTo(
        42L);
  }

  @Test
  void whenCatchupIntervalMillis_thenDefault() {
    assertThat(config(emptyMap()).getCatchupTimer().getIntervalMillis()).isEqualTo(30000L);
  }

  @Test
  void whenCatchupIntervalMillis_thenInitialized() {
    assertThat(
        config(ImmutableMap.of("http.timer.catchup.interval.millis", "73")).getCatchupTimer().getIntervalMillis()).isEqualTo(
        73L);
  }

  @Test
  void whenDeprecatedCatchupIntervalMillis_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.throttler.catchup.interval.millis", "73")).getCatchupTimer()
        .getIntervalMillis()).isEqualTo(73L);
  }

  private static AdaptableIntervalTimerConfig config(Map<String, Object> settings) {
    return new AdaptableIntervalTimerConfig(settings);
  }
}

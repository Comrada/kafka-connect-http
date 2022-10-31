package io.github.comrada.kafka.connect.timer;

import static io.github.comrada.kafka.connect.timer.FixedIntervalTimerTest.Fixture.intervalMillis;
import static io.github.comrada.kafka.connect.timer.FixedIntervalTimerTest.Fixture.lastPollMillis;
import static io.github.comrada.kafka.connect.timer.FixedIntervalTimerTest.Fixture.maxExecutionTimeMillis;
import static java.time.Instant.now;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import io.github.comrada.kafka.connect.http.model.Offset;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FixedIntervalTimerTest {

  FixedIntervalTimer timer;

  @Mock
  Function<Map<String, ?>, FixedIntervalTimerConfig> configFactory;

  @Mock
  FixedIntervalTimerConfig config;

  @Mock
  Supplier<Long> lastPollMillisInitializer;

  @Test
  void givenConfiguredTimer_whenGetRemainingMillis_thenInterval() {

    givenConfiguredTimer(intervalMillis, lastPollMillis);

    assertThat(timer.getRemainingMillis()).isCloseTo(intervalMillis, offset(maxExecutionTimeMillis));
  }

  @Test
  void givenConfiguredTimer_whenGetRemainingMillisAfterInterval_thenZero() {

    givenConfiguredTimer(intervalMillis, lastPollMillis - intervalMillis);

    assertThat(timer.getRemainingMillis()).isEqualTo(0);
  }

  private void givenConfiguredTimer(long intervalMillis, long lastPollMillis) {
    given(configFactory.apply(any())).willReturn(config);
    given(config.getPollIntervalMillis()).willReturn(intervalMillis);
    given(lastPollMillisInitializer.get()).willReturn(lastPollMillis);
    timer = new FixedIntervalTimer(configFactory, lastPollMillisInitializer);
    timer.configure(emptyMap());
  }

  interface Fixture {

    Offset offset = Offset.of(emptyMap(), "key", now());
    long intervalMillis = 300000L;
    long lastPollMillis = System.currentTimeMillis();
    long maxExecutionTimeMillis = 500L;
  }
}

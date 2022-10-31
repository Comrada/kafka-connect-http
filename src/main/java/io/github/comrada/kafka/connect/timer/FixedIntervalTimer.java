package io.github.comrada.kafka.connect.timer;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;

import io.github.comrada.kafka.connect.timer.spi.Timer;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FixedIntervalTimer implements Timer {

  private final Function<Map<String, ?>, FixedIntervalTimerConfig> configFactory;

  @Getter
  private Long intervalMillis;

  private Long lastPollMillis;

  public FixedIntervalTimer() {
    this(FixedIntervalTimerConfig::new);
  }

  FixedIntervalTimer(Function<Map<String, ?>, FixedIntervalTimerConfig> configFactory) {
    this(configFactory, System::currentTimeMillis);
  }

  FixedIntervalTimer(Function<Map<String, ?>, FixedIntervalTimerConfig> configFactory,
      Supplier<Long> lastPollMillisInitializer) {
    this.configFactory = requireNonNull(configFactory);
    this.lastPollMillis = requireNonNull(lastPollMillisInitializer.get());
  }

  @Override
  public void configure(Map<String, ?> settings) {
    FixedIntervalTimerConfig config = configFactory.apply(settings);
    this.intervalMillis = config.getPollIntervalMillis();
  }

  @Override
  public Long getRemainingMillis() {
    long now = currentTimeMillis();
    long sinceLastMillis = now - lastPollMillis;
    return max(intervalMillis - sinceLastMillis, 0);
  }

  @Override
  public void reset(Instant lastZero) {
    lastPollMillis = currentTimeMillis();
  }
}

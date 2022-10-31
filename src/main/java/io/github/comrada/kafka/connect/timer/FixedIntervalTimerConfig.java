package io.github.comrada.kafka.connect.timer;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;

import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class FixedIntervalTimerConfig extends AbstractConfig {

  @Deprecated
  private static final String DEPRECATED_INTERVAL_MILLIS = "http.throttler.interval.millis";
  static final String TIMER_INTERVAL_MILLIS = "http.timer.interval.millis";
  private static final long DEFAULT_TAIL_INTERVAL_MILLIS = 60000L;

  private final Long pollIntervalMillis;

  public FixedIntervalTimerConfig(Map<String, ?> originals) {
    super(config(), originals);
    pollIntervalMillis = resolvePollIntervalMillis();
  }

  private Long resolvePollIntervalMillis() {
    Long value = getLong(TIMER_INTERVAL_MILLIS);
    if (!value.equals(DEFAULT_TAIL_INTERVAL_MILLIS)) {
      return value;
    } else {
      return getLong(DEPRECATED_INTERVAL_MILLIS);
    }
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(TIMER_INTERVAL_MILLIS, LONG, DEFAULT_TAIL_INTERVAL_MILLIS, HIGH, "Timer Interval Millis")
        .define(DEPRECATED_INTERVAL_MILLIS, LONG, DEFAULT_TAIL_INTERVAL_MILLIS, HIGH, "(Deprecated) Timer Interval Millis");
  }
}

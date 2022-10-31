package io.github.comrada.kafka.connect.timer;

import static java.util.Objects.requireNonNull;

import io.github.comrada.kafka.connect.timer.spi.Sleeper;
import io.github.comrada.kafka.connect.timer.spi.Throttler;
import io.github.comrada.kafka.connect.timer.spi.Timer;
import java.time.Instant;
import lombok.Getter;

public class TimerThrottler implements Throttler {

  @Getter
  private final Timer timer;

  private final Sleeper sleeper;

  public TimerThrottler(Timer timer) {
    this(timer, Thread::sleep);
  }

  public TimerThrottler(Timer timer, Sleeper sleeper) {
    this.timer = requireNonNull(timer);
    this.sleeper = requireNonNull(sleeper);
  }

  @Override
  public void throttle() throws InterruptedException {
    Long remainingMillis = timer.getRemainingMillis();
    if (remainingMillis > 0) {
      sleeper.sleep(remainingMillis);
    }
  }

  public void throttle(Instant lastZero) throws InterruptedException {
    timer.reset(lastZero);
    throttle();
  }
}

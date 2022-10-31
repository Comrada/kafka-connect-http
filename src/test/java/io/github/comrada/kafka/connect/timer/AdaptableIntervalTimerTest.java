package io.github.comrada.kafka.connect.timer;

import static io.github.comrada.kafka.connect.timer.AdaptableIntervalTimerTest.Fixture.intervalMillis;
import static io.github.comrada.kafka.connect.timer.AdaptableIntervalTimerTest.Fixture.now;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AdaptableIntervalTimerTest {

  AdaptableIntervalTimer timer;

  @Mock
  AdaptableIntervalTimerConfig config;

  @Mock
  FixedIntervalTimer tailTimer;

  @Mock
  FixedIntervalTimer catchupTimer;

  @BeforeEach
  void setUp() {
    given(tailTimer.getIntervalMillis()).willReturn(intervalMillis);
    given(config.getTailTimer()).willReturn(tailTimer);
    given(config.getCatchupTimer()).willReturn(catchupTimer);
    timer = new AdaptableIntervalTimer(__ -> config);
    timer.configure(emptyMap());
  }

  @Test
  void givenFirst_whenGetRemainingMillis_thenCatchupTimer() {

    timer.getRemainingMillis();

    then(tailTimer).should(never()).getRemainingMillis();
    then(catchupTimer).should().getRemainingMillis();
  }

  @Test
  void givenNewRecordsLastNotLongAgo_whenGetRemainingMillis_thenTailTimer() {

    timer.reset(now.minus(intervalMillis - 1, MILLIS));
    timer.reset(now);

    timer.getRemainingMillis();

    then(tailTimer).should().getRemainingMillis();
    then(catchupTimer).should(never()).getRemainingMillis();
  }

  @Test
  void givenNewRecordsLastLongAgo_whenGetRemainingMillis_thenCatchupTimer() {

    timer.reset(now.minus(intervalMillis + 2, MILLIS));
    timer.reset(now.minus(intervalMillis + 1, MILLIS));

    timer.getRemainingMillis();

    then(catchupTimer).should().getRemainingMillis();
    then(tailTimer).should(never()).getRemainingMillis();
  }

  @Test
  void givenNoNewRecordsLastNotLongAgo_whenGetRemainingMillis_thenTailTimer() {

    timer.reset(now.minus(intervalMillis - 1, MILLIS));
    timer.reset(now.minus(intervalMillis - 1, MILLIS));

    timer.getRemainingMillis();

    then(catchupTimer).should(never()).getRemainingMillis();
    then(tailTimer).should().getRemainingMillis();
  }

  @Test
  void givenNoNewRecordsLastLongAgo_whenGetRemainingMillis_thenTailTimer() {

    timer.reset(now.minus(intervalMillis + 1, MILLIS));
    timer.reset(now.minus(intervalMillis + 1, MILLIS));

    timer.getRemainingMillis();

    then(catchupTimer).should(never()).getRemainingMillis();
    then(tailTimer).should().getRemainingMillis();
  }

  @Test
  void whenReset_thenTimersReset() {

    timer.reset(now);

    then(catchupTimer).should().reset(now);
    then(tailTimer).should().reset(now);
  }

  interface Fixture {

    Instant now = now();
    long intervalMillis = 60000L;
  }
}

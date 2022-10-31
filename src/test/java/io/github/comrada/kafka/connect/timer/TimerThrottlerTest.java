package io.github.comrada.kafka.connect.timer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

import io.github.comrada.kafka.connect.timer.spi.Sleeper;
import io.github.comrada.kafka.connect.timer.spi.Timer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimerThrottlerTest {

  @InjectMocks
  TimerThrottler sleeper;

  @Mock
  Timer timer;

  @Mock
  Sleeper externalSleeper;

  @Test
  void givenTimerZero_whenSleep_thenDoNotSleep() throws InterruptedException {

    given(timer.getRemainingMillis()).willReturn(0L);

    sleeper.throttle();

    then(externalSleeper).should(never()).sleep(any());
  }

  @Test
  void givenTimer_whenSleep_thenSleepForTime() throws InterruptedException {

    given(timer.getRemainingMillis()).willReturn(42L);

    sleeper.throttle();

    then(externalSleeper).should().sleep(42L);
  }
}

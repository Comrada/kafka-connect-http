package io.github.comrada.kafka.connect.http.ack;

import static io.github.comrada.kafka.connect.http.ack.ConfirmationWindowTest.Fixture.offsetMap;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfirmationWindowTest {

  @Test
  void givenConfirmationWindowInitializedWithNoOffsets_whenNoOffsetConfirmed_thenNullIsReturned() {
    ConfirmationWindow<Map<String, Object>> confirmationWindow = new ConfirmationWindow<>(emptyList());

    assertThat(confirmationWindow.getLowWatermarkOffset()).isEmpty();
  }

  @Test
  void givenConfirmationWindowInitializedWithOffsets_whenNoOffsetConfirmed_thenNullIsReturned() {
    ConfirmationWindow<Map<String, Object>> confirmationWindow =
        new ConfirmationWindow<>(asList(offsetMap(1), offsetMap(2), offsetMap(3)));

    assertThat(confirmationWindow.getLowWatermarkOffset()).isEmpty();
  }

  @Test
  void givenConfirmationWindowInitializedWithOffsets_whenAllOffsetsConfirmed_thenLastOffsetIsReturned() {
    ConfirmationWindow<Map<String, Object>> confirmationWindow =
        new ConfirmationWindow<>(asList(offsetMap(1), offsetMap(2), offsetMap(3)));

    confirmationWindow.confirm(offsetMap(1));
    confirmationWindow.confirm(offsetMap(2));
    confirmationWindow.confirm(offsetMap(3));

    assertThat(confirmationWindow.getLowWatermarkOffset()).contains(offsetMap(3));
  }

  @Test
  void givenConfirmationWindowInitializedWithOffsets_whenAllOffsetsConfirmedOutOfOrder_thenLastOffsetIsReturned() {
    ConfirmationWindow<Map<String, Object>> confirmationWindow =
        new ConfirmationWindow<>(asList(offsetMap(1), offsetMap(2), offsetMap(3)));

    confirmationWindow.confirm(offsetMap(2));
    confirmationWindow.confirm(offsetMap(3));
    confirmationWindow.confirm(offsetMap(1));

    assertThat(confirmationWindow.getLowWatermarkOffset()).contains(offsetMap(3));
  }

  @Test
  void givenConfirmationWindowInitializedWithOffsets_whenFirstOffsetsNotConfirmed_thenNullIsReturned() {
    ConfirmationWindow<Map<String, Object>> confirmationWindow =
        new ConfirmationWindow<>(asList(offsetMap(1), offsetMap(2), offsetMap(3)));

    confirmationWindow.confirm(offsetMap(2));
    confirmationWindow.confirm(offsetMap(3));

    assertThat(confirmationWindow.getLowWatermarkOffset()).isEmpty();
  }

  @Test
  void givenConfirmationWindowInitializedWithOffsets_whenSecondOffsetsNotConfirmed_thenFirstOffsetIsReturned() {
    ConfirmationWindow<Map<String, Object>> confirmationWindow =
        new ConfirmationWindow<>(asList(offsetMap(1), offsetMap(2), offsetMap(3)));

    confirmationWindow.confirm(offsetMap(1));
    confirmationWindow.confirm(offsetMap(3));

    assertThat(confirmationWindow.getLowWatermarkOffset()).contains(offsetMap(1));
  }

  interface Fixture {

    Instant now = now();
    String key = "customKey";

    static Map<String, Object> offsetMap(Object value) {
      return ImmutableMap.of("custom", value, "key", key, "timestamp", now.toString());
    }
  }
}

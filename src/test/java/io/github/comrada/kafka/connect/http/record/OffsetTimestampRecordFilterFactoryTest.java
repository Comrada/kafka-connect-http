package io.github.comrada.kafka.connect.http.record;

import static io.github.comrada.kafka.connect.http.record.OffsetTimestampRecordFilterFactoryTest.Fixture.key;
import static io.github.comrada.kafka.connect.http.record.OffsetTimestampRecordFilterFactoryTest.Fixture.now;
import static io.github.comrada.kafka.connect.http.record.OffsetTimestampRecordFilterFactoryTest.Fixture.record;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.comrada.kafka.connect.http.model.Offset;
import java.time.Instant;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OffsetTimestampRecordFilterFactoryTest {

  OffsetTimestampRecordFilterFactory factory = new OffsetTimestampRecordFilterFactory();

  @Test
  void givenOffset_whenTestEarlier_thenFalse() {
    assertThat(factory.create(Offset.of(emptyMap(), key, now)).test(record(now.minus(1, MINUTES)))).isFalse();
  }

  @Test
  void givenOffset_whenTestLater_thenTrue() {
    assertThat(factory.create(Offset.of(emptyMap(), key, now)).test(record(now.plus(1, MINUTES)))).isTrue();
  }

  interface Fixture {

    String key = "key";
    Instant now = now();

    static SourceRecord record(Instant timestamp) {
      return new SourceRecord(null, null, null, null, null, null, null, null, timestamp.toEpochMilli());
    }
  }
}

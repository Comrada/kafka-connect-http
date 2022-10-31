package io.github.comrada.kafka.connect.http.record;

import static io.github.comrada.kafka.connect.http.record.StringKvSourceRecordMapperTest.Fixture.now;
import static io.github.comrada.kafka.connect.http.record.StringKvSourceRecordMapperTest.Fixture.offset;
import static io.github.comrada.kafka.connect.http.record.StringKvSourceRecordMapperTest.Fixture.record;
import static java.time.Instant.now;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StringKvSourceRecordMapperTest {

  StringKvSourceRecordMapper mapper;

  @Mock
  SourceRecordMapperConfig config;

  @BeforeEach
  void setUp() {
    given(config.getTopic()).willReturn("topic");
    mapper = new StringKvSourceRecordMapper(__ -> config);
    mapper.configure(emptyMap());
  }

  @Test
  void givenTopic_whenMap_thenTopicMapped() {
    assertThat(mapper.map(record).topic()).isEqualTo("topic");
  }

  @Test
  void givenKey_whenMap_thenIdMapped() {
    assertThat(mapper.map(record.withKey("value")).key()).isEqualTo("value");
  }

  @Test
  void givenValue_whenMap_thenBodyMapped() {
    assertThat(mapper.map(record.withValue("value")).value()).isEqualTo("value");
  }

  @Test
  void givenOffset_whenMap_thenOffsetMapped() {
    assertThat(mapper.map(record.withOffset(offset)).sourceOffset()).isEqualTo(offset.toMap());
  }

  @Test
  void givenTimestamp_whenMap_thenTimestampMapped() {
    assertThat(mapper.map(record.withOffset(offset)).timestamp()).isEqualTo(now.toEpochMilli());
  }

  @Test
  void whenMap_thenNoPartitionMapped() {
    assertThat(mapper.map(record).kafkaPartition()).isNull();
  }

  @Test
  void whenMap_thenKeySchemaMapped() {
    assertThat(mapper.map(record).keySchema()).isNotNull();
  }

  @Test
  void whenMap_thenValueSchemaMapped() {
    assertThat(mapper.map(record).valueSchema()).isNotNull();
  }

  interface Fixture {

    Instant now = now();
    Offset offset = Offset.of(ImmutableMap.of("k", "v"), "key", now);
    KvRecord record = KvRecord.builder().value("not-null").offset(offset).build();
  }
}

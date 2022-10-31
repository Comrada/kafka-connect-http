package io.github.comrada.kafka.connect.http.record;

import static io.github.comrada.kafka.connect.http.record.SchemedKvSourceRecordMapperTest.Fixture.now;
import static io.github.comrada.kafka.connect.http.record.SchemedKvSourceRecordMapperTest.Fixture.offset;
import static io.github.comrada.kafka.connect.http.record.SchemedKvSourceRecordMapperTest.Fixture.record;
import static java.time.Instant.now;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemedKvSourceRecordMapperTest {

  SchemedKvSourceRecordMapper mapper;

  @Mock
  SourceRecordMapperConfig config;

  @BeforeEach
  void setUp() {
    given(config.getTopic()).willReturn("topic");
    mapper = new SchemedKvSourceRecordMapper(__ -> config);
    mapper.configure(emptyMap());
  }

  @Test
  void givenTopic_whenMap_thenTopicMapped() {
    assertThat(mapper.map(record).topic()).isEqualTo("topic");
  }

  @Test
  void givenKey_whenMap_thenKeyMapped() {
    assertThat(((Struct) mapper.map(record.withKey("key")).key()).get("key")).isEqualTo("key");
  }

  @Test
  void givenValue_whenMap_thenValueMapped() {
    assertThat(((Struct) mapper.map(record.withValue("value")).value()).get("value")).isEqualTo("value");
  }

  @Test
  void givenKey_whenMap_thenValueKeyMapped() {
    assertThat(((Struct) mapper.map(record.withKey("key")).value()).get("key")).isEqualTo("key");
  }

  @Test
  void givenOffsetTimestamp_whenMap_thenValueTimestampMapped() {
    assertThat(((Struct) mapper.map(record.withOffset(offset)).value()).get("timestamp")).isEqualTo(now.toEpochMilli());
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

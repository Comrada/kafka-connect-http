package io.github.comrada.kafka.connect.http.response.jackson;

import static io.github.comrada.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParserTest.Fixture.bytes;
import static io.github.comrada.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParserTest.Fixture.record;
import static io.github.comrada.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParserTest.Fixture.response;
import static io.github.comrada.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParserTest.Fixture.timestamp;
import static io.github.comrada.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParserTest.Fixture.timestampIso;
import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.parse;
import static java.util.Collections.emptyMap;
import static java.util.UUID.nameUUIDFromBytes;
import static java.util.stream.Stream.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import io.github.comrada.kafka.connect.http.response.jackson.model.JacksonRecord;
import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JacksonKvRecordHttpResponseParserTest {

  JacksonKvRecordHttpResponseParser parser;

  @Mock
  JacksonKvRecordHttpResponseParserConfig config;

  @Mock
  JacksonResponseRecordParser responseParser;

  @Mock
  TimestampParser timestampParser;

  @BeforeEach
  void setUp() {
    parser = new JacksonKvRecordHttpResponseParser(__ -> config);
    given(config.getResponseParser()).willReturn(responseParser);
    given(config.getTimestampParser()).willReturn(timestampParser);
    parser.configure(emptyMap());
  }

  @Test
  void givenNoItems_thenEmpty() {

    givenRecords(empty());

    assertThat(parser.parse(response)).isEmpty();
  }

  @Test
  void givenOneItem_thenKeyMapped() {

    givenRecords(Stream.of(record.withKey("key")));

    assertThat(parser.parse(response)).first().extracting(KvRecord::getKey).isEqualTo("key");
  }

  @Test
  void givenOneItemWithNoKeyButOffset_thenKeyMappedFromOffset() {

    givenRecords(Stream.of(record.withKey(null).withOffset(ImmutableMap.of("key", "value"))));

    assertThat(parser.parse(response)).first().extracting(KvRecord::getKey).isEqualTo("value");
  }

  @Test
  void givenOneItemWithNoNoKey_thenKeyDefault() {

    givenRecords(Stream.of(record.withKey(null)));

    assertThat(parser.parse(response)).first().extracting(KvRecord::getKey).isNotNull();
  }

  @Test
  void givenOneItem_thenValueMapped() {

    givenRecords(Stream.of(record.withBody("value")));

    assertThat(parser.parse(response)).first().extracting(KvRecord::getValue).isEqualTo("value");
  }

  @Test
  void givenOneItem_thenTimestampMapped() {

    givenRecords(Stream.of(record.withTimestamp(timestampIso)));
    given(timestampParser.parse(timestampIso)).willReturn(timestamp);

    assertThat(parser.parse(response)).first().extracting(KvRecord::getOffset).extracting(Offset::getTimestamp)
        .isEqualTo(Optional.of(timestamp));
  }

  @Test
  void givenOneItemWitNoTimestampButOffset_thenTimestampMappedFromOffset() {

    givenRecords(Stream.of(record.withTimestamp(null).withOffset(ImmutableMap.of("timestamp", timestampIso))));
    given(timestampParser.parse(timestampIso)).willReturn(timestamp);

    assertThat(parser.parse(response)).first().extracting(KvRecord::getOffset).extracting(Offset::getTimestamp)
        .isEqualTo(Optional.of(timestamp));
  }

  @Test
  void givenOneItemWithNoTimestamp_thenDefault() {

    givenRecords(Stream.of(record.withTimestamp(null)));

    assertThat(parser.parse(response)).first().extracting(KvRecord::getOffset).extracting(Offset::getTimestamp).isNotNull();
  }

  @Test
  void givenOneItem_thenOffsetMapped() {

    givenRecords(Stream.of(record.withOffset(ImmutableMap.of("offset-key", "offset-value"))));

    assertThat(parser.parse(response).stream().findFirst().get().getOffset().toMap().get("offset-key")).isEqualTo(
        "offset-value");
  }

  @Test
  void givenOneItem_thenTimestampMappedToOffset() {

    givenRecords(Stream.of(record.withTimestamp(timestampIso).withOffset(emptyMap())));
    given(timestampParser.parse(timestampIso)).willReturn(timestamp);

    assertThat(parser.parse(response).stream().findFirst().get().getOffset().getTimestamp()).contains(parse(timestampIso));
  }

  @Test
  void givenOneItem_thenKeyMappedToOffset() {

    givenRecords(Stream.of(record.withKey("value").withOffset(emptyMap())));

    assertThat(parser.parse(response).stream().findFirst().get().getOffset().getKey()).contains("value");
  }

  @Test
  void givenOneItemWithNoKey_thenConsistentUUIDMappedToOffset() {

    givenRecords(Stream.of(record.withKey(null).withOffset(emptyMap())));

    assertThat(parser.parse(response).stream().findFirst().get().getOffset().getKey()).contains(
        nameUUIDFromBytes(record.getBody().toString().getBytes()).toString());
  }

  private void givenRecords(Stream<JacksonRecord> records) {
    given(responseParser.getRecords(bytes)).willReturn(records);
  }

  interface Fixture {

    byte[] bytes = "bytes".getBytes();
    HttpResponse response = HttpResponse.builder().body(bytes).build();
    Instant timestamp = ofEpochMilli(43L);
    String timestampIso = timestamp.toString();
    JacksonRecord record = JacksonRecord.builder().body("Binary Value").build();
  }
}

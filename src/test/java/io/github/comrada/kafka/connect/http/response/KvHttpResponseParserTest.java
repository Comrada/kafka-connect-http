package io.github.comrada.kafka.connect.http.response;

import static io.github.comrada.kafka.connect.http.response.KvHttpResponseParserTest.Fixture.record;
import static io.github.comrada.kafka.connect.http.response.KvHttpResponseParserTest.Fixture.response;
import static io.github.comrada.kafka.connect.http.response.KvHttpResponseParserTest.Fixture.sourceRecord;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import io.github.comrada.kafka.connect.http.record.spi.KvSourceRecordMapper;
import io.github.comrada.kafka.connect.http.response.spi.KvRecordHttpResponseParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KvHttpResponseParserTest {

  KvHttpResponseParser parser;

  @Mock
  KvHttpResponseParserConfig config;

  @Mock
  KvRecordHttpResponseParser recordParser;

  @Mock
  KvSourceRecordMapper recordFactory;

  @BeforeEach
  void setUp() {
    parser = new KvHttpResponseParser(__ -> config);
    given(config.getRecordParser()).willReturn(recordParser);
    given(config.getRecordMapper()).willReturn(recordFactory);
    parser.configure(emptyMap());
  }

  @Test
  void givenEmptyList_whenParse_thenEmpty() {

    given(recordParser.parse(response)).willReturn(emptyList());

    assertThat(parser.parse(response)).isEmpty();
  }

  @Test
  void givenList_whenParse_thenItemsMapped() {

    given(recordParser.parse(response)).willReturn(singletonList(record));

    parser.parse(response);

    then(recordFactory).should().map(record);
  }

  @Test
  void givenEmptyList_whenParse_thenItemsMappedReturned() {

    given(recordParser.parse(response)).willReturn(singletonList(record));
    given(recordFactory.map(record)).willReturn(sourceRecord);

    assertThat(parser.parse(response)).containsExactly(sourceRecord);
  }

  interface Fixture {

    HttpResponse response = HttpResponse.builder().build();
    KvRecord record = KvRecord.builder().build();
    SourceRecord sourceRecord = new SourceRecord(null, null, null, null, null);
  }
}

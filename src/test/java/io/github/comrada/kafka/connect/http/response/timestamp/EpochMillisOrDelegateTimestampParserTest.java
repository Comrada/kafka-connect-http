package io.github.comrada.kafka.connect.http.response.timestamp;

import static java.time.Instant.ofEpochMilli;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EpochMillisOrDelegateTimestampParserTest {

  EpochMillisOrDelegateTimestampParser parser;

  @Mock
  TimestampParser delegate;

  @Mock
  EpochMillisOrDelegateTimestampParserConfig config;

  @BeforeEach
  void setUp() {
    parser = new EpochMillisOrDelegateTimestampParser(__ -> config);
    given(config.getDelegateParser()).willReturn(delegate);
    parser.configure(emptyMap());
  }

  @Test
  void givenNumber_whenParse_thenParsed() {
    assertThat(parser.parse("123")).isEqualTo(ofEpochMilli(123));
  }

  @Test
  void givenNotNumber_whenParse_thenDelegated() {

    parser.parse("abc");

    then(delegate).should().parse("abc");
  }

  @Test
  void givenNotNumber_whenParse_thenReturnedFromDelegate() {

    given(delegate.parse("abc")).willReturn(ofEpochMilli(123));

    assertThat(parser.parse("abc")).isEqualTo(ofEpochMilli(123));
  }
}

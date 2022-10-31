package io.github.comrada.kafka.connect.http.response.jackson;

import static io.github.comrada.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParserConfigTest.Fixture.config;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.comrada.kafka.connect.http.response.timestamp.EpochMillisOrDelegateTimestampParser;
import io.github.comrada.kafka.connect.http.response.timestamp.EpochMillisTimestampParser;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JacksonKvRecordHttpResponseParserConfigTest {

  @Test
  void whenItemsParserClassConfigured_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.record.parser",
        "io.github.comrada.kafka.connect.http.response.jackson.")).getResponseParser())
        .isInstanceOf(JacksonResponseRecordParser.class);
  }

  @Test
  void whenMissingItemParserClassConfigured_thenInitialized() {
    assertThat(config(emptyMap()).getResponseParser()).isInstanceOf(JacksonResponseRecordParser.class);
  }

  @Test
  void whenTimestampParserClassConfigured_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.record.timestamp.parser",
        "io.github.comrada.kafka.connect.http.response.timestamp.EpochMillisTimestampParser")).getTimestampParser())
        .isInstanceOf(EpochMillisTimestampParser.class);
  }

  @Test
  void whenMissingTimestampParserClassConfigured_thenInitialized() {
    assertThat(config(emptyMap()).getTimestampParser()).isInstanceOf(EpochMillisOrDelegateTimestampParser.class);
  }

  interface Fixture {

    static JacksonKvRecordHttpResponseParserConfig config(Map<String, String> settings) {
      return new JacksonKvRecordHttpResponseParserConfig(settings);
    }
  }
}

package io.github.comrada.kafka.connect.http.response.timestamp;

import static io.github.comrada.kafka.connect.http.response.timestamp.EpochMillisOrDelegateTimestampParserConfigTest.Fixture.config;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EpochMillisOrDelegateTimestampParserConfigTest {

  @Test
  void whenTimestampParserClassConfigured_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.record.timestamp.parser.delegate",
        "io.github.comrada.kafka.connect.http.response.timestamp.EpochMillisTimestampParser")).getDelegateParser())
        .isInstanceOf(EpochMillisTimestampParser.class);
  }

  @Test
  void whenMissingTimestampParserClassConfigured_thenInitialized() {
    assertThat(config(emptyMap()).getDelegateParser()).isInstanceOf(DateTimeFormatterTimestampParser.class);
  }

  interface Fixture {

    static EpochMillisOrDelegateTimestampParserConfig config(Map<String, String> settings) {
      return new EpochMillisOrDelegateTimestampParserConfig(settings);
    }
  }
}

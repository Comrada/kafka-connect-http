package io.github.comrada.kafka.connect.http.response.timestamp;

import static io.github.comrada.kafka.connect.http.response.timestamp.RegexTimestampParserConfigTest.Fixture.config;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class RegexTimestampParserConfigTest {

  @Test
  void whenItemTimestampParserDelegateClassConfigured_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.record.timestamp.parser.regex.delegate",
        "io.github.comrada.kafka.connect.http.response.timestamp.EpochMillisTimestampParser")).getDelegateParser())
        .isInstanceOf(EpochMillisTimestampParser.class);
    assertThat(config(ImmutableMap.of("http.response.record.timestamp.parser.regex.delegate",
        "io.github.comrada.kafka.connect.http.response.timestamp.EpochMillisTimestampParser")).getTimestampRegex())
        .isEqualTo(".*");
  }

  @Test
  void whenItemTimestampParserRegexConfigured_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.record.timestamp.parser.regex",
        "(?:\\/Date\\()(.*?)(?:\\+0000\\)\\/)")).getDelegateParser())
        .isInstanceOf(DateTimeFormatterTimestampParser.class);
    assertThat(config(ImmutableMap.of("http.response.record.timestamp.parser.regex",
        "(?:\\/Date\\()(.*?)(?:\\+0000\\)\\/)")).getTimestampRegex())
        .isEqualTo("(?:\\/Date\\()(.*?)(?:\\+0000\\)\\/)");
  }


  interface Fixture {

    static RegexTimestampParserConfig config(Map<String, String> settings) {
      return new RegexTimestampParserConfig(settings);
    }
  }
}

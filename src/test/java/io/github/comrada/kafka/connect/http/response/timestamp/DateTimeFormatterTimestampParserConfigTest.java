package io.github.comrada.kafka.connect.http.response.timestamp;

import static io.github.comrada.kafka.connect.http.response.timestamp.DateTimeFormatterTimestampParserConfigTest.Fixture.config;
import static io.github.comrada.kafka.connect.http.response.timestamp.DateTimeFormatterTimestampParserConfigTest.Fixture.date;
import static io.github.comrada.kafka.connect.http.response.timestamp.DateTimeFormatterTimestampParserConfigTest.Fixture.isoDate;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DateTimeFormatterTimestampParserConfigTest {

  @Test
  void whenItemTimestampParserPatternConfigured_thenInitialized() {
    assertThat(
        config(ImmutableMap.of("http.response.record.timestamp.parser.pattern", "yyyy-MM-dd")).getRecordTimestampFormatter()
            .parse(date).toString())
        .isEqualTo(ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC")).parse(date).toString());
  }

  @Test
  void whenItemTimestampParserZoneConfigured_thenInitialized() {
    assertThat(config(
        ImmutableMap.of("http.response.record.timestamp.parser.zone", "America/New_York")).getRecordTimestampFormatter()
        .parse(isoDate).toString())
        .isEqualTo(
            ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneId.of("America/New_York")).parse(isoDate).toString());
  }

  @Test
  void whenMissingItemTimestampParserClassConfigured_thenInitialized() {
    assertThat(config(emptyMap()).getRecordTimestampFormatter()).isInstanceOf(DateTimeFormatter.class);
  }

  interface Fixture {

    String date = "2020-01-01";
    String isoDate = "2020-01-01T01:23:45.000Z";

    static DateTimeFormatterTimestampParserConfig config(Map<String, String> settings) {
      return new DateTimeFormatterTimestampParserConfig(settings);
    }
  }
}

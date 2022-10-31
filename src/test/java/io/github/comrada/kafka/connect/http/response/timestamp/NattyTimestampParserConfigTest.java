package io.github.comrada.kafka.connect.http.response.timestamp;

import static io.github.comrada.kafka.connect.http.response.timestamp.NattyTimestampParserConfigTest.Fixture.config;
import static io.github.comrada.kafka.connect.http.response.timestamp.NattyTimestampParserConfigTest.Fixture.zoneId;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.time.ZoneId;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NattyTimestampParserConfigTest {

  @Test
  void whenItemsParserClassConfigured_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.record.timestamp.parser.zone", zoneId)).getTimestampZoneId()).contains(
        ZoneId.of(zoneId));
  }

  @Test
  void whenMissingItemParserClassConfigured_thenDefault() {
    assertThat(config(emptyMap()).getTimestampZoneId()).isEmpty();
  }

  interface Fixture {

    String zoneId = "Europe/Paris";

    static NattyTimestampParserConfig config(Map<String, String> settings) {
      return new NattyTimestampParserConfig(settings);
    }
  }
}

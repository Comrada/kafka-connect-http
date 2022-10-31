package io.github.comrada.kafka.connect.http.record;

import static io.github.comrada.kafka.connect.http.record.SourceRecordMapperConfigTest.Fixture.minimumConfig;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class SourceRecordMapperConfigTest {

  @Test
  void whenMissingKafkaTopic_thenException() {
    assertThat(catchThrowable(() -> new SourceRecordMapperConfig(emptyMap()))).isInstanceOf(ConfigException.class);
  }

  @Test
  void whenKafkaTopic_thenInitialized() {
    assertThat(minimumConfig(emptyMap()).getTopic()).isEqualTo("test-topic");
  }

  interface Fixture {

    static SourceRecordMapperConfig minimumConfig(Map<String, String> customConfig) {
      HashMap<String, String> finalConfig = new HashMap<>();
      finalConfig.put("kafka.topic", "test-topic");
      finalConfig.putAll(customConfig);
      return new SourceRecordMapperConfig(finalConfig);
    }
  }
}

package io.github.comrada.kafka.connect.http;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class HttpSourceConnectorTest {

  HttpSourceConnector connector = new HttpSourceConnector();

  @Test
  void whenTaskClass_thenHttpSourceTask() {
    assertThat(connector.taskClass()).isEqualTo(HttpSourceTask.class);
  }

  @Test
  void whenConfig_thenNotEmpty() {
    assertThat(connector.config().configKeys()).isNotEmpty();
  }

  @Test
  void whenSeveralTaskConfigs_thenAsManyReturned() {
    assertThat(connector.taskConfigs(3)).hasSize(3);
  }

  @Test
  void whenSeveralTaskConfigs_thenAllWithConnectorConfig() {

    ImmutableMap<String, String> myMap = ImmutableMap.of("key", "value");

    connector.start(myMap);

    assertThat(connector.taskConfigs(3)).containsExactly(myMap, myMap, myMap);
  }

  @Test
  void whenStop_thenSettingsNull() {

    connector.stop();

    assertThat(connector.taskConfigs(1)).isEqualTo(singletonList(null));
  }

  @Test
  void whenGetVersion_thenNotEmpty() {
    assertThat(connector.version()).isNotEmpty();
  }
}

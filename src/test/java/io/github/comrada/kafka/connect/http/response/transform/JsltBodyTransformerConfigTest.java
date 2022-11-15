package io.github.comrada.kafka.connect.http.response.transform;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsltBodyTransformerConfigTest {

  @Test
  void whenNoDelegate_thenDefault() {
    assertThat(config(emptyMap()).getResponseTransformExpression()).isEqualTo(".");
  }

  @Test
  void whenDelegate_thenInitialized() {
    assertThat(config(singletonMap("http.response.transform.jslt", ".")).getResponseTransformExpression())
        .isEqualTo(".");
  }

  private static JsltBodyTransformerConfig config(Map<String, Object> settings) {
    Map<String, Object> defaultSettings = new HashMap<>(settings);
    defaultSettings.put("kafka.topic", "topic");
    return new JsltBodyTransformerConfig(defaultSettings);
  }
}
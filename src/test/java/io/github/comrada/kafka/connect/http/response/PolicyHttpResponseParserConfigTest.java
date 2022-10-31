package io.github.comrada.kafka.connect.http.response;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponseParser;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class PolicyHttpResponseParserConfigTest {

  @Test
  void whenNoDelegate_thenDefault() {
    assertThat(config(emptyMap()).getDelegateParser()).isInstanceOf(KvHttpResponseParser.class);
  }

  @Test
  void whenDelegate_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.policy.parser",
        TestResponseParser.class.getName())).getDelegateParser()).isInstanceOf(TestResponseParser.class);
  }

  @Test
  void whenNoPolicy_thenDefault() {
    assertThat(config(emptyMap()).getPolicy()).isInstanceOf(StatusCodeHttpResponsePolicy.class);
  }

  @Test
  void whenPolicy_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.policy", TestPolicy.class.getName())).getPolicy()).isInstanceOf(
        TestPolicy.class);
  }

  public static class TestResponseParser implements HttpResponseParser {

    @Override
    public List<SourceRecord> parse(HttpResponse response) {
      return null;
    }
  }

  public static class TestPolicy implements HttpResponsePolicy {

    @Override
    public HttpResponseOutcome resolve(HttpResponse response) {
      return null;
    }
  }

  private static PolicyHttpResponseParserConfig config(Map<String, Object> settings) {
    Map<String, Object> defaultSettings = new HashMap<String, Object>() {{
      put("kafka.topic", "topic");
    }};
    defaultSettings.putAll(settings);
    return new PolicyHttpResponseParserConfig(defaultSettings);
  }
}

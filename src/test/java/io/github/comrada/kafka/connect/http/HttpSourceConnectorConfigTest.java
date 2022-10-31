package io.github.comrada.kafka.connect.http;

import static io.github.comrada.kafka.connect.http.HttpSourceConnectorConfigTest.Fixture.config;
import static io.github.comrada.kafka.connect.http.HttpSourceConnectorConfigTest.Fixture.configWithout;
import static java.util.Collections.emptyMap;

import io.github.comrada.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.comrada.kafka.connect.http.client.spi.HttpClient;
import io.github.comrada.kafka.connect.http.model.HttpRequest;
import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.OffsetRecordFilterFactory;
import io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter;
import io.github.comrada.kafka.connect.http.record.PassthroughRecordFilterFactory;
import io.github.comrada.kafka.connect.http.record.spi.SourceRecordSorter;
import io.github.comrada.kafka.connect.http.request.spi.HttpRequestFactory;
import io.github.comrada.kafka.connect.http.request.template.TemplateHttpRequestFactory;
import io.github.comrada.kafka.connect.http.response.PolicyHttpResponseParser;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponseParser;
import io.github.comrada.kafka.connect.timer.AdaptableIntervalTimer;
import io.github.comrada.kafka.connect.timer.FixedIntervalTimer;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class HttpSourceConnectorConfigTest {

  @Test
  void whenNoTimer_thenDefault() {
    Assertions.assertThat(configWithout("http.timer").getThrottler().getTimer()).isInstanceOf(AdaptableIntervalTimer.class);
  }

  @Test
  void whenTimer_thenInitialized() {
    Assertions.assertThat(
            config("http.timer", "io.github.comrada.kafka.connect.timer.FixedIntervalTimer").getThrottler().getTimer())
        .isInstanceOf(FixedIntervalTimer.class);
  }

  @Test
  void whenNoClient_thenDefault() {
    Assertions.assertThat(configWithout("http.client").getClient()).isInstanceOf(OkHttpClient.class);
  }

  @Test
  void whenClient_thenInitialized() {
    Assertions.assertThat(config("http.client", TestHttpClient.class.getName()).getClient())
        .isInstanceOf(TestHttpClient.class);
  }

  @Test
  void whenNoRequestFactory_thenDefault() {
    Assertions.assertThat(configWithout("http.request.factory").getRequestFactory())
        .isInstanceOf(TemplateHttpRequestFactory.class);
  }

  @Test
  void whenRequestFactory_thenInitialized() {
    Assertions.assertThat(config("http.request.factory", TestRequestFactory.class.getName()).getRequestFactory())
        .isInstanceOf(TestRequestFactory.class);
  }

  @Test
  void whenNoResponseParser_thenDefault() {
    Assertions.assertThat(configWithout("http.response.parser").getResponseParser())
        .isInstanceOf(PolicyHttpResponseParser.class);
  }

  @Test
  void whenResponseParser_thenInitialized() {
    Assertions.assertThat(config("http.response.parser", TestResponseParser.class.getName()).getResponseParser())
        .isInstanceOf(TestResponseParser.class);
  }

  @Test
  void whenNoRecordSorter_thenDefault() {
    Assertions.assertThat(configWithout("http.record.sorter").getRecordSorter())
        .isInstanceOf(OrderDirectionSourceRecordSorter.class);
  }

  @Test
  void whenRecordSorter_thenInitialized() {
    Assertions.assertThat(config("http.record.sorter", TestRecordSorter.class.getName()).getRecordSorter())
        .isInstanceOf(TestRecordSorter.class);
  }

  @Test
  void whenNoResponseFilterFactory_thenDefault() {
    Assertions.assertThat(configWithout("http.record.filter.factory").getRecordFilterFactory())
        .isInstanceOf(OffsetRecordFilterFactory.class);
  }

  @Test
  void whenResponseFilterFactory_thenInitialized() {
    Assertions.assertThat(config("http.record.filter.factory",
            "io.github.comrada.kafka.connect.http.record.PassthroughRecordFilterFactory").getRecordFilterFactory())
        .isInstanceOf(PassthroughRecordFilterFactory.class);
  }

  @Test
  void whenNoInitialOffset_thenDefault() {
    Assertions.assertThat(configWithout("http.offset.initial").getInitialOffset()).isEqualTo(emptyMap());
  }

  @Test
  void whenInitialOffset_thenInitialized() {
    Assertions.assertThat(config("http.offset.initial", "k=v").getInitialOffset()).isEqualTo(ImmutableMap.of("k", "v"));
  }

  public static class TestHttpClient implements HttpClient {

    public HttpResponse execute(HttpRequest request) {
      return null;
    }
  }

  public static class TestRequestFactory implements HttpRequestFactory {

    public HttpRequest createRequest(Offset offset) {
      return null;
    }
  }

  public static class TestResponseParser implements HttpResponseParser {

    public List<SourceRecord> parse(HttpResponse response) {
      return null;
    }
  }

  public static class TestRecordSorter implements SourceRecordSorter {

    public List<SourceRecord> sort(List<SourceRecord> records) {
      return null;
    }
  }

  interface Fixture {

    static Map<String, String> defaultMap() {
      return new HashMap<String, String>() {{
        put("kafka.topic", "topic");
        put("http.request.url", "foo");
        put("http.response.json.record.offset.value.pointer", "/baz");
      }};
    }

    static HttpSourceConnectorConfig config(String key, String value) {
      Map<String, String> customMap = defaultMap();
      customMap.put(key, value);
      return new HttpSourceConnectorConfig(customMap);
    }

    static HttpSourceConnectorConfig configWithout(String key) {
      Map<String, String> customMap = defaultMap();
      customMap.remove(key);
      return new HttpSourceConnectorConfig(customMap);
    }
  }
}

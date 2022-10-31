package io.github.comrada.kafka.connect.http.response;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.record.SchemedKvSourceRecordMapper;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import io.github.comrada.kafka.connect.http.record.spi.KvSourceRecordMapper;
import io.github.comrada.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParser;
import io.github.comrada.kafka.connect.http.response.spi.KvRecordHttpResponseParser;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class KvHttpResponseParserConfigTest {

  @Test
  void whenNoDelegate_thenDefault() {
    assertThat(config(emptyMap()).getRecordParser()).isInstanceOf(JacksonKvRecordHttpResponseParser.class);
  }

  @Test
  void whenDelegate_thenInitialized() {
    assertThat(config(
        ImmutableMap.of("http.response.record.parser", TestResponseParser.class.getName())).getRecordParser()).isInstanceOf(
        TestResponseParser.class);
  }

  @Test
  void whenNoPolicy_thenDefault() {
    assertThat(config(emptyMap()).getRecordMapper()).isInstanceOf(SchemedKvSourceRecordMapper.class);
  }

  @Test
  void whenPolicy_thenInitialized() {
    assertThat(config(
        ImmutableMap.of("http.response.record.mapper", TestRecordMapper.class.getName())).getRecordMapper()).isInstanceOf(
        TestRecordMapper.class);
  }

  public static class TestResponseParser implements KvRecordHttpResponseParser {

    @Override
    public List<KvRecord> parse(HttpResponse response) {
      return null;
    }
  }

  public static class TestRecordMapper implements KvSourceRecordMapper {

    @Override
    public SourceRecord map(KvRecord record) {
      return null;
    }
  }

  private static KvHttpResponseParserConfig config(Map<String, Object> settings) {
    Map<String, Object> defaultSettings = new HashMap<String, Object>() {{
      put("kafka.topic", "topic");
    }};
    defaultSettings.putAll(settings);
    return new KvHttpResponseParserConfig(defaultSettings);
  }
}

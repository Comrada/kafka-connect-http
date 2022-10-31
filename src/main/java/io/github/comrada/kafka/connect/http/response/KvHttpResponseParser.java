package io.github.comrada.kafka.connect.http.response;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.record.spi.KvSourceRecordMapper;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponseParser;
import io.github.comrada.kafka.connect.http.response.spi.KvRecordHttpResponseParser;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.source.SourceRecord;

public class KvHttpResponseParser implements HttpResponseParser {

  private final Function<Map<String, ?>, KvHttpResponseParserConfig> configFactory;

  private KvRecordHttpResponseParser recordParser;
  private KvSourceRecordMapper recordMapper;

  KvHttpResponseParser(Function<Map<String, ?>, KvHttpResponseParserConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public KvHttpResponseParser() {
    this(KvHttpResponseParserConfig::new);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    KvHttpResponseParserConfig config = configFactory.apply(configs);
    recordParser = config.getRecordParser();
    recordMapper = config.getRecordMapper();
  }

  @Override
  public List<SourceRecord> parse(HttpResponse response) {
    return recordParser.parse(response).stream()
        .map(recordMapper::map)
        .collect(toList());
  }
}

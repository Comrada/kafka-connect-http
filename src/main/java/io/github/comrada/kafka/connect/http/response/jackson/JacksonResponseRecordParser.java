package io.github.comrada.kafka.connect.http.response.jackson;

import static io.github.comrada.kafka.connect.common.CollectionUtils.merge;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.comrada.kafka.connect.http.response.jackson.model.JacksonRecord;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.kafka.common.Configurable;

public class JacksonResponseRecordParser implements Configurable {

  private final Function<Map<String, ?>, JacksonRecordParserConfig> configFactory;

  private final JacksonRecordParser recordParser;

  private final JacksonSerializer serializer;

  private JsonPointer recordsPointer;

  public JacksonResponseRecordParser() {
    this(new JacksonRecordParser(), new JacksonSerializer(new ObjectMapper()));
  }

  public JacksonResponseRecordParser(JacksonRecordParser recordParser, JacksonSerializer serializer) {
    this(JacksonRecordParserConfig::new, recordParser, serializer);
  }

  public JacksonResponseRecordParser(Function<Map<String, ?>, JacksonRecordParserConfig> configFactory,
      JacksonRecordParser recordParser, JacksonSerializer serializer) {
    this.configFactory = requireNonNull(configFactory);
    this.recordParser = requireNonNull(recordParser);
    this.serializer = requireNonNull(serializer);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    JacksonRecordParserConfig config = configFactory.apply(settings);
    recordsPointer = config.getRecordsPointer();
  }

  Stream<JacksonRecord> getRecords(byte[] body) {
    JsonNode jsonBody = serializer.deserialize(body);
    Map<String, Object> responseOffset = getResponseOffset(jsonBody);
    return serializer.getArrayAt(jsonBody, recordsPointer)
        .map(jsonRecord -> toJacksonRecord(jsonRecord, responseOffset));
  }

  private Map<String, Object> getResponseOffset(JsonNode node) {
    return emptyMap();
  }

  private JacksonRecord toJacksonRecord(JsonNode jsonRecord, Map<String, Object> responseOffset) {
    return JacksonRecord.builder()
        .key(recordParser.getKey(jsonRecord).orElse(null))
        .timestamp(recordParser.getTimestamp(jsonRecord).orElse(null))
        .offset(merge(responseOffset, recordParser.getOffset(jsonRecord)))
        .body(recordParser.getValue(jsonRecord))
        .build();
  }
}

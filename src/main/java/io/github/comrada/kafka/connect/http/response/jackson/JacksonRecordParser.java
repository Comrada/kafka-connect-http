package io.github.comrada.kafka.connect.http.response.jackson;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.Configurable;

public class JacksonRecordParser implements Configurable {

  private final Function<Map<String, ?>, JacksonRecordParserConfig> configFactory;

  private final JacksonSerializer serializer;

  private List<JsonPointer> keyPointer;
  private Optional<JsonPointer> timestampPointer;
  private Map<String, JsonPointer> offsetPointers;
  private JsonPointer valuePointer;

  public JacksonRecordParser() {
    this(new JacksonSerializer(new ObjectMapper()));
  }

  public JacksonRecordParser(JacksonSerializer serializer) {
    this(JacksonRecordParserConfig::new, serializer);
  }

  public JacksonRecordParser(Function<Map<String, ?>, JacksonRecordParserConfig> configFactory,
      JacksonSerializer serializer) {
    this.configFactory = requireNonNull(configFactory);
    this.serializer = requireNonNull(serializer);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    JacksonRecordParserConfig config = configFactory.apply(settings);
    keyPointer = config.getKeyPointer();
    valuePointer = config.getValuePointer();
    offsetPointers = config.getOffsetPointers();
    timestampPointer = config.getTimestampPointer();
  }

  /**
   * @deprecated Replaced by Offset
   */
  @Deprecated
  Optional<String> getKey(JsonNode node) {
    String key = keyPointer.stream()
        .map(pointer -> serializer.getObjectAt(node, pointer).asText())
        .filter(it -> !it.isEmpty())
        .collect(joining("+"));
    return key.isEmpty() ? Optional.empty() : Optional.of(key);
  }

  /**
   * @deprecated Replaced by Offset
   */
  @Deprecated
  Optional<String> getTimestamp(JsonNode node) {
    return timestampPointer.map(pointer -> serializer.getObjectAt(node, pointer).asText());
  }

  Map<String, Object> getOffset(JsonNode node) {
    return offsetPointers.entrySet().stream()
        .collect(toMap(Entry::getKey, entry -> serializer.getObjectAt(node, entry.getValue()).asText()));
  }

  String getValue(JsonNode node) {
    JsonNode value = serializer.getObjectAt(node, valuePointer);
    return value.isObject() ? serializer.serialize(value) : value.asText();
  }
}

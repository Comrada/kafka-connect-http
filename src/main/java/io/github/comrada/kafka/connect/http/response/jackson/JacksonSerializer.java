package io.github.comrada.kafka.connect.http.response.jackson;

import static com.fasterxml.jackson.core.JsonPointer.compile;
import static java.util.stream.StreamSupport.stream;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
class JacksonSerializer {

  private static final JsonPointer JSON_ROOT = compile("/");

  private final ObjectMapper objectMapper;

  public JacksonSerializer() {
    this(new ObjectMapper());
  }

  @SneakyThrows(IOException.class)
  JsonNode deserialize(byte[] body) {
    return objectMapper.readTree(body);
  }

  @SneakyThrows(IOException.class)
  String serialize(JsonNode node) {
    return objectMapper.writeValueAsString(node);
  }

  JsonNode getObjectAt(JsonNode node, JsonPointer pointer) {
    return getRequiredAt(node, pointer);
  }

  Stream<JsonNode> getArrayAt(JsonNode node, JsonPointer pointer) {
    JsonNode array = getRequiredAt(node, pointer);
    if (array.isArray()) {
      return stream(array.spliterator(), false);
    } else if (array.isNull()) {
      return Stream.empty();
    }
    return Stream.of(array);
  }

  private static JsonNode getRequiredAt(JsonNode body, JsonPointer recordsPointer) {
    return JSON_ROOT.equals(recordsPointer) ? body : body.requiredAt(recordsPointer);
  }
}

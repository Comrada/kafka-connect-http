package io.github.comrada.kafka.connect.http.response.transform;

import static io.github.comrada.kafka.connect.LogsCollector.findEventWithText;
import static io.github.comrada.kafka.connect.TestUtils.loadResourceFile;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.schibsted.spt.data.jslt.JsltException;
import io.github.comrada.kafka.connect.LogsCollector;
import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JsltBodyTransformerTest {

  @Mock
  private JsltBodyTransformerConfig config;
  private JsltBodyTransformer transformer;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    transformer = new JsltBodyTransformer(__ -> config);
  }

  @BeforeAll
  static void resetLogsBefore() {
    LogsCollector.reset();
  }

  @AfterAll
  static void resetLogsAfter() {
    LogsCollector.reset();
  }

  @Test
  void transform() throws IOException {
    String response = loadResourceFile(getClass(), "response-1.json");
    String jslt = loadResourceFile(getClass(), "jslt-1.txt");
    when(config.getResponseTransformExpression()).thenReturn(jslt);
    transformer.configure(emptyMap());
    HttpResponse httpResponse = HttpResponse.builder()
        .headers(singletonMap("Content-Type", singletonList("application/json")))
        .body(response.getBytes(StandardCharsets.UTF_8))
        .build();
    HttpResponse transform = transformer.transform(
        httpResponse);
    ArrayNode arrayNode = objectMapper.readValue(transform.getBody(), ArrayNode.class);

    assertTrue(arrayNode.isArray());
    assertEquals(1, arrayNode.size());
    assertEquals("QTUMETH", arrayNode.get(0).get("symbol").asText());
  }

  @Test
  void whenJsltIsInvalid_thenExceptionThrown() {
    when(config.getResponseTransformExpression()).thenReturn("invalid JSLT");
    assertThrows(JsltException.class, () -> transformer.configure(emptyMap()));
  }

  @Test
  void whenNotJsonProvided_thenExceptionThrown() {
    when(config.getResponseTransformExpression()).thenReturn(".");
    transformer.configure(emptyMap());
    HttpResponse httpResponse = HttpResponse.builder()
        .headers(singletonMap("Content-Type", singletonList("application/xml")))
        .body("<xml>hello</xml>".getBytes(StandardCharsets.UTF_8))
        .build();
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> transformer.transform(httpResponse));
    assertEquals("The server returned an unsupported content type", exception.getMessage());
    assertTrue(findEventWithText("Server response content type: application/xml").isPresent());
  }

  @Test
  void whenResponseDoesNotHaveContentType_thenJsonIsUsed() {
    when(config.getResponseTransformExpression()).thenReturn(".");
    transformer.configure(emptyMap());
    HttpResponse httpResponse = HttpResponse.builder()
        .body("[1]".getBytes(StandardCharsets.UTF_8))
        .build();
    transformer.transform(httpResponse);

    assertTrue(findEventWithText("Server response does not contain a content type header, assuming it's JSON.").isPresent());
  }

  @Test
  void whenInvalidJsonProvided_thenExceptionThrown() {
    when(config.getResponseTransformExpression()).thenReturn(".");
    transformer.configure(emptyMap());
    HttpResponse httpResponse = HttpResponse.builder()
        .headers(singletonMap("Content-Type", singletonList("application/json")))
        .body("<xml>it's not JSON</xml>".getBytes(StandardCharsets.UTF_8))
        .build();
    assertThrows(IOException.class, () -> transformer.transform(httpResponse));
  }
}
package io.github.comrada.kafka.connect.http.response.transform;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;
import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.response.transform.spi.HttpResponseTransformer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * When the server returns JSON that the connector cannot handle (an array of mixed values, for example), you can perform a
 * preliminary transformation of the structure before the connector continues to parse the server response.
 * <p/>
 * The transformer can also be used for flexible mapping of complex JSON with nested objects.
 * <p/>
 * Example usage: Suppose the server returns the following response:
 * <pre>
 *   [
 *     [
 *         1667257200000,
 *         "0.46240000",
 *         "0.46740000",
 *         "0.46070000",
 *         "0.46410000",
 *         "28051685.00000000",
 *         1667260799999,
 *         "13048670.75260000",
 *         13362,
 *         "15114732.00000000",
 *         "7031036.24040000"
 *     ]
 *   ]
 * </pre>
 * Then we next use the JSLT transformation:
 * <pre>
 * [for (.) {
 * "open_at": .[0],
 * "open": .[1],
 * "high": .[2],
 * "low": .[3],
 * "close": .[4],
 * "volume": .[5],
 * "closed_at": .[6],
 * "quote_asset_volume": .[7],
 * "number_of_trades": .[8],
 * "taker_buy_base_asset_volume": .[9],
 * "taker_buy_quote_asset_volume": .[10]
 * }]
 * </pre>
 * Then the server response will be converted into the following structure:
 * <pre>
 * [
 *   {
 *     "open_at": 1667257200000,
 *     "open": "0.46240000",
 *     "high": "0.46740000",
 *     "low": "0.46070000",
 *     "close": "0.46410000",
 *     "volume": "28051685.00000000",
 *     "closed_at": 1667260799999,
 *     "quote_asset_volume": "13048670.75260000",
 *     "number_of_trades": 13362,
 *     "taker_buy_base_asset_volume": "15114732.00000000",
 *     "taker_buy_quote_asset_volume": "7031036.24040000",
 *     "reserved": "0"
 *   }
 * ]
 * </pre>
 * More examples of JSLT transformation <a href="https://github.com/schibsted/jslt">here</a>
 */
@Slf4j
public class JsltBodyTransformer implements HttpResponseTransformer {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String CONTENT_TYPE_HEADER = "content-type";
  private static final String SUPPORTED_CONTENT_TYPE = "application/json";
  private final Function<Map<String, ?>, JsltBodyTransformerConfig> configFactory;
  private Expression expression;

  JsltBodyTransformer(Function<Map<String, ?>, JsltBodyTransformerConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public JsltBodyTransformer() {
    this(JsltBodyTransformerConfig::new);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    JsltBodyTransformerConfig config = configFactory.apply(configs);
    this.expression = Parser.compileString(config.getResponseTransformExpression());
  }

  @Override
  @SneakyThrows(IOException.class)
  public HttpResponse transform(HttpResponse response) {
    checkContentType(response);
    JsonNode originalJsonNode = OBJECT_MAPPER.readTree(new String(response.getBody(), StandardCharsets.UTF_8));
    JsonNode transformedJsonNode = expression.apply(originalJsonNode);
    String transformed = OBJECT_MAPPER.writeValueAsString(transformedJsonNode);
    return HttpResponse.builder()
        .body(transformed.getBytes(StandardCharsets.UTF_8))
        .code(response.getCode())
        .headers(response.getHeaders())
        .build();
  }

  private void checkContentType(HttpResponse response) {
    Optional<List<String>> foundContentType = response.getHeaders().entrySet().stream()
        .filter(header -> header.getKey().equalsIgnoreCase(CONTENT_TYPE_HEADER))
        .map(Entry::getValue)
        .findFirst();
    if (foundContentType.isPresent()) {
      List<String> header = foundContentType.get();
      if (header.stream().noneMatch(value -> value.toLowerCase().contains(SUPPORTED_CONTENT_TYPE))) {
        log.error("Server response content type: {}", String.join(System.lineSeparator(), header));
        throw new IllegalArgumentException("The server returned an unsupported content type");
      }
    } else {
      log.info("Server response does not contain a content type header, assuming it's JSON.");
    }
  }
}

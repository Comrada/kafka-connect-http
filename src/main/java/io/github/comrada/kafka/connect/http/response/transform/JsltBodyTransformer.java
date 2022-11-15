package io.github.comrada.kafka.connect.http.response.transform;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;
import io.github.comrada.kafka.connect.http.header.HeaderUtils;
import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.response.transform.spi.HttpResponseTransformer;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * When the server returns JSON that the connector cannot handle (an array of mixed values, for example), you can perform a
 * preliminary transformation of the structure before the connector continues to parse the server response.
 * </p>
 * <p>
 * The transformer can also be used for flexible mapping of complex JSON with nested objects.
 * </p>
 * <p>
 * Example usage: Suppose the server returns the following response:
 * </p>
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
 * "openAt": .[0],
 * "open": number(.[1]),
 * "high": number(.[2]),
 * "low": number(.[3]),
 * "close": number(.[4]),
 * "volume": number(.[5]),
 * "closedAt": .[6],
 * "quoteAssetVolume": number(.[7]),
 * "numberOfTrades": .[8],
 * "takerBuyBaseAssetVolume": number(.[9]),
 * "takerBuyQuoteAssetVolume": number(.[10])
 * }]
 * </pre>
 * Then the server response will be converted into the following structure:
 * <pre>
 * [
 *   {
 *     "openAt": 1667257200000,
 *     "open": 0.4624,
 *     "high": 0.4674,
 *     "low": 0.4607,
 *     "close": 0.4641,
 *     "volume": 28051685,
 *     "closedAt": 1667260799999,
 *     "quoteAssetVolume": 13048670.7526,
 *     "numberOfTrades": 13362,
 *     "takerBuyBaseAssetVolume": 15114732,
 *     "takerBuyQuoteAssetVolume": 7031036.2404
 *   }
 * ]
 * </pre>
 * More examples of JSLT transformation <a href="https://github.com/schibsted/jslt">here</a>
 */
@Slf4j
public class JsltBodyTransformer implements HttpResponseTransformer {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
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
    Charset originCharset = HeaderUtils.getCharset(response);
    String transformed = doTransform(response, originCharset);
    return HttpResponse.builder()
        .body(transformed.getBytes(originCharset))
        .code(response.getCode())
        .headers(response.getHeaders())
        .build();
  }

  private String doTransform(HttpResponse response, Charset originCharset) throws JsonProcessingException {
    JsonNode originalJsonNode = OBJECT_MAPPER.readTree(new String(response.getBody(), originCharset));
    JsonNode transformedJsonNode = expression.apply(originalJsonNode);
    return OBJECT_MAPPER.writeValueAsString(transformedJsonNode);
  }

  private void checkContentType(HttpResponse response) {
    Optional<String> optionalContentType = HeaderUtils.getContentType(response);
    if (optionalContentType.isPresent()) {
      String contentValue = optionalContentType.get();
      if (!contentValue.equals(SUPPORTED_CONTENT_TYPE)) {
        log.error("Server response content type: {}", String.join(System.lineSeparator(), contentValue));
        throw new IllegalArgumentException("The server returned an unsupported content type");
      }
    } else {
      log.debug("Server response does not contain a content type header, assuming it's JSON.");
    }
  }
}

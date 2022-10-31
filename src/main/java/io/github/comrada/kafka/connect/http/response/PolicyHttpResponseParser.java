package io.github.comrada.kafka.connect.http.response;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponseParser;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.source.SourceRecord;

public class PolicyHttpResponseParser implements HttpResponseParser {

  private final Function<Map<String, ?>, PolicyHttpResponseParserConfig> configFactory;

  private HttpResponseParser delegate;

  private HttpResponsePolicy policy;

  PolicyHttpResponseParser(Function<Map<String, ?>, PolicyHttpResponseParserConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public PolicyHttpResponseParser() {
    this(PolicyHttpResponseParserConfig::new);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    PolicyHttpResponseParserConfig config = configFactory.apply(settings);
    delegate = config.getDelegateParser();
    policy = config.getPolicy();
  }

  @Override
  public List<SourceRecord> parse(HttpResponse response) {
    switch (policy.resolve(response)) {
      case PROCESS:
        return delegate.parse(response);
      case SKIP:
        return emptyList();
      case FAIL:
      default:
        throw new IllegalStateException(String.format("Policy failed for response code: %s, body: %s", response.getCode(),
            ofNullable(response.getBody()).map(String::new).orElse("")));
    }
  }
}

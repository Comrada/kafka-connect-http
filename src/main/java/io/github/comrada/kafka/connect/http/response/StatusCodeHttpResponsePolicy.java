package io.github.comrada.kafka.connect.http.response;

import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.FAIL;
import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.PROCESS;
import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.SKIP;
import static java.util.Objects.requireNonNull;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatusCodeHttpResponsePolicy implements HttpResponsePolicy {

  private final Function<Map<String, ?>, StatusCodeHttpResponsePolicyConfig> configFactory;

  private Set<Integer> processCodes;

  private Set<Integer> skipCodes;

  StatusCodeHttpResponsePolicy(Function<Map<String, ?>, StatusCodeHttpResponsePolicyConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public StatusCodeHttpResponsePolicy() {
    this(StatusCodeHttpResponsePolicyConfig::new);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    StatusCodeHttpResponsePolicyConfig config = configFactory.apply(settings);
    processCodes = config.getProcessCodes();
    skipCodes = config.getSkipCodes();
  }

  @Override
  public HttpResponseOutcome resolve(HttpResponse response) {
    if (processCodes.contains(response.getCode())) {
      return PROCESS;
    } else if (skipCodes.contains(response.getCode())) {
      log.warn("Unexpected HttpResponse status code: {}, continuing with no records", response.getCode());
      return SKIP;
    } else {
      return FAIL;
    }
  }
}

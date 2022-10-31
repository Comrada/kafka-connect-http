package io.github.comrada.kafka.connect.http.response.spi;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@FunctionalInterface
public interface HttpResponsePolicy extends Configurable {

  HttpResponseOutcome resolve(HttpResponse response);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }

  enum HttpResponseOutcome {
    PROCESS, SKIP, FAIL
  }
}

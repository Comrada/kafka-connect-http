package io.github.comrada.kafka.connect.http.request.spi;

import io.github.comrada.kafka.connect.http.model.HttpRequest;
import io.github.comrada.kafka.connect.http.model.Offset;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@FunctionalInterface
public interface HttpRequestFactory extends Configurable {

  HttpRequest createRequest(Offset offset);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

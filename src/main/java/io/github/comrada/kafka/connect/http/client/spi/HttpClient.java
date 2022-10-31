package io.github.comrada.kafka.connect.http.client.spi;

import io.github.comrada.kafka.connect.http.model.HttpRequest;
import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@FunctionalInterface
public interface HttpClient extends Configurable {

  HttpResponse execute(HttpRequest request) throws IOException;

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

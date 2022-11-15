package io.github.comrada.kafka.connect.http.response.transform.spi;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * The server response may contain content that is unsuitable or inconvenient for this connector. With a transformer, you can
 * transform the server response before the connector continues to do its work.
 */
public interface HttpResponseTransformer extends Configurable {

  /**
   * Transforms the server response and returns a new one with changed content or headers.
   *
   * @param response the original server response
   * @return modified response
   */
  HttpResponse transform(HttpResponse response);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

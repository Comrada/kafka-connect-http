package io.github.comrada.kafka.connect.http.auth.spi;

import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Configurable;

@FunctionalInterface
public interface HttpAuthenticator extends Configurable {

  Optional<String> getAuthorizationHeader();

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

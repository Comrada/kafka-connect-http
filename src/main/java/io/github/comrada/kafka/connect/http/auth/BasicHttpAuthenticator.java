package io.github.comrada.kafka.connect.http.auth;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang.StringUtils.isEmpty;

import io.github.comrada.kafka.connect.http.auth.spi.HttpAuthenticator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import okhttp3.Credentials;

public class BasicHttpAuthenticator implements HttpAuthenticator {

  private final Function<Map<String, ?>, BasicHttpAuthenticatorConfig> configFactory;
  private String header;

  public BasicHttpAuthenticator() {
    this(BasicHttpAuthenticatorConfig::new);
  }

  public BasicHttpAuthenticator(Function<Map<String, ?>, BasicHttpAuthenticatorConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    BasicHttpAuthenticatorConfig config = configFactory.apply(configs);
    if (!isEmpty(config.getUser()) || !isEmpty(config.getPassword().value())) {
      header = Credentials.basic(config.getUser(), config.getPassword().value());
    } else {
      header = null;
    }
  }

  @Override
  public Optional<String> getAuthorizationHeader() {
    return Optional.ofNullable(header);
  }
}

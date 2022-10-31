package io.github.comrada.kafka.connect.http.auth;

import static java.util.Objects.requireNonNull;

import io.github.comrada.kafka.connect.http.auth.spi.HttpAuthenticator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class ConfigurableHttpAuthenticator implements HttpAuthenticator {

  private final Function<Map<String, ?>, ConfigurableHttpAuthenticatorConfig> configFactory;

  private HttpAuthenticator delegate;

  public ConfigurableHttpAuthenticator() {
    this(ConfigurableHttpAuthenticatorConfig::new);
  }

  ConfigurableHttpAuthenticator(Function<Map<String, ?>, ConfigurableHttpAuthenticatorConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    ConfigurableHttpAuthenticatorConfig config = configFactory.apply(configs);
    delegate = config.getAuthenticator();
  }

  @Override
  public Optional<String> getAuthorizationHeader() {
    return delegate.getAuthorizationHeader();
  }
}

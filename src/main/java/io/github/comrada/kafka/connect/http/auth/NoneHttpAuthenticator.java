package io.github.comrada.kafka.connect.http.auth;

import static java.util.Optional.empty;

import io.github.comrada.kafka.connect.http.auth.spi.HttpAuthenticator;
import java.util.Optional;

public class NoneHttpAuthenticator implements HttpAuthenticator {

  @Override
  public Optional<String> getAuthorizationHeader() {
    return empty();
  }
}

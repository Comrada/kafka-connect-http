package io.github.comrada.kafka.connect.http.auth;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import io.github.comrada.kafka.connect.http.auth.spi.HttpAuthenticator;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigurableHttpAuthenticatorTest {

  @Mock
  ConfigurableHttpAuthenticatorConfig config;

  @Mock
  HttpAuthenticator delegate;

  ConfigurableHttpAuthenticator authenticator;

  @BeforeEach
  void setUp() {
    authenticator = new ConfigurableHttpAuthenticator(__ -> config);
  }

  @Test
  void whenHeader_thenDelegated() {

    given(config.getAuthenticator()).willReturn(delegate);
    given(delegate.getAuthorizationHeader()).willReturn(Optional.of("header"));

    authenticator.configure(emptyMap());

    assertThat(authenticator.getAuthorizationHeader()).contains("header");
  }
}

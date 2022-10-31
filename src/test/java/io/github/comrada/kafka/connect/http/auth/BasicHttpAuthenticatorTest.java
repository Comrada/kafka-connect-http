package io.github.comrada.kafka.connect.http.auth;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BasicHttpAuthenticatorTest {

  @Mock
  BasicHttpAuthenticatorConfig config;

  BasicHttpAuthenticator authenticator;

  @BeforeEach
  void setUp() {
    authenticator = new BasicHttpAuthenticator(__ -> config);
  }

  @Test
  void whenCredentials_thenHeader() {

    given(config.getUser()).willReturn("user");
    given(config.getPassword()).willReturn(new Password("password"));

    authenticator.configure(emptyMap());

    assertThat(authenticator.getAuthorizationHeader()).contains("Basic dXNlcjpwYXNzd29yZA==");
  }

  @Test
  void whenNoCredentials_thenHeaderEmpty() {

    given(config.getUser()).willReturn("");
    given(config.getPassword()).willReturn(new Password(""));

    authenticator.configure(emptyMap());

    assertThat(authenticator.getAuthorizationHeader()).isEmpty();
  }
}

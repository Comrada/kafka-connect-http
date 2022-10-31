package io.github.comrada.kafka.connect.http.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NoneHttpAuthenticatorTest {

  @Test
  void whenGetHeader_thenEmpty() {
    assertThat(new NoneHttpAuthenticator().getAuthorizationHeader()).isEmpty();
  }
}

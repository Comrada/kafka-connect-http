package io.github.comrada.kafka.connect.http.auth;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

class BasicHttpAuthenticatorConfigTest {

  @Test
  void whenNoUser_thenDefault() {
    assertThat(config(emptyMap()).getUser()).isEqualTo("");
  }

  @Test
  void whenUser_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.auth.user", "user")).getUser()).isEqualTo("user");
  }

  @Test
  void whenNoPassword_thenDefault() {
    assertThat(config(emptyMap()).getPassword()).isEqualTo(new Password(""));
  }

  @Test
  void whenPassword_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.auth.password", "password")).getPassword()).isEqualTo(new Password("password"));
  }

  private static BasicHttpAuthenticatorConfig config(Map<String, String> config) {
    return new BasicHttpAuthenticatorConfig(config);
  }
}

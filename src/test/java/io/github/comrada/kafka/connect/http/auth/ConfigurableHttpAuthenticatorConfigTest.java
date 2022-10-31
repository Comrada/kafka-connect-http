package io.github.comrada.kafka.connect.http.auth;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigurableHttpAuthenticatorConfigTest {

  @Test
  void whenNoType_thenDefault() {
    assertThat(config(emptyMap()).getAuthenticator()).isInstanceOf(NoneHttpAuthenticator.class);
  }

  @Test
  void whenNoneType_thenBasic() {
    assertThat(config(ImmutableMap.of("http.auth.type", "None")).getAuthenticator()).isInstanceOf(
        NoneHttpAuthenticator.class);
  }

  @Test
  void whenBasicType_thenBasic() {
    assertThat(config(ImmutableMap.of("http.auth.type", "Basic")).getAuthenticator()).isInstanceOf(
        BasicHttpAuthenticator.class);
  }

  private static ConfigurableHttpAuthenticatorConfig config(Map<String, String> config) {
    return new ConfigurableHttpAuthenticatorConfig(config);
  }
}

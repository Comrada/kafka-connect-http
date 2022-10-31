package io.github.comrada.kafka.connect.http.response;

import static io.github.comrada.kafka.connect.http.response.StatusCodeHttpResponsePolicyTest.Fixture.code;
import static io.github.comrada.kafka.connect.http.response.StatusCodeHttpResponsePolicyTest.Fixture.response;
import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.FAIL;
import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.PROCESS;
import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.SKIP;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StatusCodeHttpResponsePolicyTest {

  StatusCodeHttpResponsePolicy policy;

  @Mock
  StatusCodeHttpResponsePolicyConfig config;

  @BeforeEach
  void setUp() {
    policy = new StatusCodeHttpResponsePolicy(__ -> config);
  }

  @Test
  void givenCodeProcess_whenResolve_thenProcess() {

    given(config.getProcessCodes()).willReturn(Stream.of(code).collect(toSet()));
    policy.configure(emptyMap());

    assertThat(policy.resolve(response.withCode(code))).isEqualTo(PROCESS);
  }

  @Test
  void givenCodeSkip_whenResolve_thenSkip() {

    given(config.getSkipCodes()).willReturn(Stream.of(code).collect(toSet()));
    policy.configure(emptyMap());

    assertThat(policy.resolve(response.withCode(code))).isEqualTo(SKIP);
  }

  @Test
  void givenCodeNoProcessNorSkip_whenResolve_thenFail() {

    given(config.getProcessCodes()).willReturn(emptySet());
    given(config.getSkipCodes()).willReturn(emptySet());
    policy.configure(emptyMap());

    assertThat(policy.resolve(response.withCode(code))).isEqualTo(FAIL);
  }

  interface Fixture {

    HttpResponse response = HttpResponse.builder().build();
    int code = 200;
  }
}

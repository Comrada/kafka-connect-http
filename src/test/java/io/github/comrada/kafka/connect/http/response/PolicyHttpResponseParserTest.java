package io.github.comrada.kafka.connect.http.response;

import static io.github.comrada.kafka.connect.http.response.PolicyHttpResponseParserTest.Fixture.record;
import static io.github.comrada.kafka.connect.http.response.PolicyHttpResponseParserTest.Fixture.response;
import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.FAIL;
import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.PROCESS;
import static io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy.HttpResponseOutcome.SKIP;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponseParser;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponsePolicy;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PolicyHttpResponseParserTest {

  PolicyHttpResponseParser parser;

  @Mock
  PolicyHttpResponseParserConfig config;

  @Mock
  HttpResponseParser delegate;

  @Mock
  HttpResponsePolicy policy;

  @BeforeEach
  void setUp() {
    parser = new PolicyHttpResponseParser(__ -> config);
    given(config.getDelegateParser()).willReturn(delegate);
    given(config.getPolicy()).willReturn(policy);
    parser.configure(emptyMap());
  }

  @Test
  void givenPolicyFail_whenParse_thenIllegalState() {

    given(policy.resolve(response)).willReturn(FAIL);

    assertThat(catchThrowable(() -> parser.parse(response))).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void givenPolicyFail_whenParse_thenDontDelegate() {

    given(policy.resolve(response)).willReturn(FAIL);

    catchThrowable(() -> parser.parse(response));

    then(delegate).should(never()).parse(any());
  }

  @Test
  void givenPolicyProcess_whenParse_thenDelegate() {

    given(policy.resolve(response)).willReturn(PROCESS);

    parser.parse(response);

    then(delegate).should().parse(response);
  }

  @Test
  void givenPolicyProcess_whenParse_thenResponseFromDelegate() {

    given(policy.resolve(response)).willReturn(PROCESS);

    given(delegate.parse(response)).willReturn(ImmutableList.of(record));

    assertThat(parser.parse(response)).containsExactly(record);
  }

  @Test
  void givenPolicySkip_whenParse_thenDontDelegate() {

    given(policy.resolve(response)).willReturn(SKIP);

    parser.parse(response);

    then(delegate).should(never()).parse(any());
  }

  @Test
  void givenPolicySkip_whenParse_thenEmptyList() {

    given(policy.resolve(response)).willReturn(SKIP);

    assertThat(parser.parse(response)).isEmpty();
  }

  interface Fixture {

    HttpResponse response = HttpResponse.builder().build();
    SourceRecord record = new SourceRecord(null, null, null, null, "Something");
  }
}

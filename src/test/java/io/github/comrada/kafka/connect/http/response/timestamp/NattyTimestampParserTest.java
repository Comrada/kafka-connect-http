package io.github.comrada.kafka.connect.http.response.timestamp;

import static java.time.Instant.parse;
import static java.util.Collections.emptyMap;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import java.time.ZoneId;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
class NattyTimestampParserTest {

  NattyTimestampParser parser;

  @Mock
  NattyTimestampParserConfig config;

  @BeforeEach
  void setUp() {
    parser = new NattyTimestampParser(__ -> config);
  }

  @Test
  void givenDateWithoutTimezone_whenParse_thenParsedWithLocalTimezone() {

    given(config.getTimestampZoneId()).willReturn(empty());
    parser.configure(emptyMap());

    assertThat(parser.parse("2020-04-28T00:19:05"))
        .isEqualTo(
            parse("2020-04-28T00:19:05Z").atZone(ZoneId.of("UTC")).withZoneSameLocal(ZoneId.systemDefault()).toInstant());
  }

  @Test
  void givenDateWithTimezone_whenParse_thenParsedWithGivenTimezone() {

    given(config.getTimestampZoneId()).willReturn(Optional.of(ZoneId.of("America/New_York")));
    parser.configure(emptyMap());

    assertThat(parser.parse("2020-04-28T00:19:05"))
        .isEqualTo(parse("2020-04-28T04:19:05Z"));
  }
}

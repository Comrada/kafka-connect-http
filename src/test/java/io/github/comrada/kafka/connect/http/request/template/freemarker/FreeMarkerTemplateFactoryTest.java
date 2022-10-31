package io.github.comrada.kafka.connect.http.request.template.freemarker;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.comrada.kafka.connect.http.model.Offset;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class FreeMarkerTemplateFactoryTest {

  FreeMarkerTemplateFactory factory = new FreeMarkerTemplateFactory();

  @Test
  void givenTemplate_whenApplyEmpty_thenAsIs() {
    assertThat(factory.create("template").apply(Offset.of(emptyMap()))).isEqualTo("template");
  }

  @Test
  void givenTemplate_whenApplyValue_thenReplaced() {
    Offset offset = Offset.of(ImmutableMap.of("key", "offset1"));
    assertThat(factory.create("template ${offset.key}").apply(offset)).isEqualTo("template offset1");
  }

  @Test
  void givenTemplateWithTimestampAsString_whenApplyValue_thenReplaced() {
    Offset offset = Offset.of(ImmutableMap.of("timestamp", Instant.parse("2020-01-01T00:00:00Z")));
    assertThat(factory.create("${offset.timestamp}").apply(offset)).isEqualTo("2020-01-01T00:00:00Z");
  }

  @Test
  void givenTemplateWithTimestampAsEpoch_whenApplyValue_thenReplaced() {
    Offset offset = Offset.of(ImmutableMap.of("timestamp", Instant.parse("2020-01-01T00:00:00Z")));
    assertThat(factory.create("${offset.timestamp?datetime.iso?long}").apply(offset)).isEqualTo("1577836800000");
  }
}

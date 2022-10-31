package io.github.comrada.kafka.connect.http.request.template;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.comrada.kafka.connect.http.model.Offset;
import org.junit.jupiter.api.Test;

class NoTemplateFactoryTest {

  NoTemplateFactory factory = new NoTemplateFactory();

  @Test
  void givenTemplate_whenApply_thenAsIs() {
    assertThat(factory.create("template").apply(Offset.of(emptyMap()))).isEqualTo("template");
  }
}

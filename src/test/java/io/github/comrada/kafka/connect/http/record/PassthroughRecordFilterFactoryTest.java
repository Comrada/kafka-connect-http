package io.github.comrada.kafka.connect.http.record;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PassthroughRecordFilterFactoryTest {

  PassthroughRecordFilterFactory factory = new PassthroughRecordFilterFactory();

  @Test
  void givenFactoryWithNull_whenTestNull_thenTrue() {
    assertThat(factory.create(null).test(null)).isTrue();
  }
}

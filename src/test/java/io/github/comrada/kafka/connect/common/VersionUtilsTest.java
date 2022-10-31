package io.github.comrada.kafka.connect.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class VersionUtilsTest {

  @Test
  void givenFileWithVersion_thenFileVersion() {
    assertThat(VersionUtils.getVersion()).isNotNull();
  }
}

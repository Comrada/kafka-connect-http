package io.github.comrada.kafka.connect.common;

import static io.github.comrada.kafka.connect.TestUtils.loadResourceFile;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class VersionUtilsTest {

  @Test
  void givenFileWithVersion_thenFileVersion() throws IOException {
    String expected = loadResourceFile(VersionUtils.class, "/version.properties").replace("version=", "");

    assertThat(VersionUtils.getVersion())
        .isNotNull()
        .isEqualTo(expected);
  }
}

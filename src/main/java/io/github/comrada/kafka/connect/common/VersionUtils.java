package io.github.comrada.kafka.connect.common;

import java.io.InputStream;
import java.util.Properties;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class VersionUtils {

  private static final String PATH = "/version.properties";
  private static String version = "unknown";

  static {
    try (InputStream stream = VersionUtils.class.getResourceAsStream(PATH)) {
      Properties props = new Properties();
      props.load(stream);
      version = props.getProperty("version", version).trim();
    } catch (Exception e) {
      log.warn("Error while loading version:", e);
    }
  }

  public static String getVersion() {
    return version;
  }
}

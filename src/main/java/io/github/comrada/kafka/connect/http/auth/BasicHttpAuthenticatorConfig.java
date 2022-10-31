package io.github.comrada.kafka.connect.http.auth;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

@Getter
public class BasicHttpAuthenticatorConfig extends AbstractConfig {

  private static final String USER = "http.auth.user";
  private static final String PASSWORD = "http.auth.password";

  private final String user;
  private final Password password;

  BasicHttpAuthenticatorConfig(Map<String, ?> originals) {
    super(config(), originals);
    user = getString(USER);
    password = getPassword(PASSWORD);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(USER, STRING, "", HIGH, "Username")
        .define(PASSWORD, ConfigDef.Type.PASSWORD, "", HIGH, "Password");
  }
}

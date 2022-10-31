package io.github.comrada.kafka.connect.http.response;

import static io.github.comrada.kafka.connect.common.ConfigUtils.parseIntegerRangedList;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class StatusCodeHttpResponsePolicyConfig extends AbstractConfig {

  private static final String PROCESS_CODES = "http.response.policy.codes.process";
  private static final String SKIP_CODES = "http.response.policy.codes.skip";

  private final Set<Integer> processCodes;
  private final Set<Integer> skipCodes;

  public StatusCodeHttpResponsePolicyConfig(Map<String, ?> originals) {
    super(config(), originals);
    processCodes = parseIntegerRangedList(getString(PROCESS_CODES));
    skipCodes = parseIntegerRangedList(getString(SKIP_CODES));
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(PROCESS_CODES, STRING, "200..299", HIGH, "Process Codes")
        .define(SKIP_CODES, STRING, "300..399", HIGH, "Skip Codes");
  }
}

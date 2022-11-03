package io.github.comrada.kafka.connect.http.response.transform;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class JsltBodyTransformerConfig extends AbstractConfig {

  private static final String RESPONSE_TRANSFORMATION_JSLT = "http.response.transform.jslt";
  private final String responseTransformExpression;

  public JsltBodyTransformerConfig(Map<?, ?> originals) {
    super(config(), originals);
    responseTransformExpression = getString(RESPONSE_TRANSFORMATION_JSLT);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(RESPONSE_TRANSFORMATION_JSLT, STRING, ".", LOW, "JSLT expression for Response Transformation");
  }
}

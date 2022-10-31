package io.github.comrada.kafka.connect.http.response.jackson.model;

import static java.util.Collections.emptyMap;

import java.util.Map;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.With;

@With
@Value
@Builder
public class JacksonRecord {

  /**
   * @deprecated To be integrated in offset
   */
  @Deprecated
  String key;

  /**
   * @deprecated To be integrated in offset
   */
  @Deprecated
  String timestamp;

  @Default
  Map<String, Object> offset = emptyMap();

  String body;
}

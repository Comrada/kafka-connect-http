package io.github.comrada.kafka.connect.http.model;

import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.With;

@With
@Value
@Builder
public class HttpResponse {

  Integer code;

  byte[] body;

  @Default
  Map<String, List<String>> headers = emptyMap();
}

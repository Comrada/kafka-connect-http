package io.github.comrada.kafka.connect.http.model;

import static io.github.comrada.kafka.connect.http.model.HttpRequest.HttpMethod.GET;
import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
public class HttpRequest {

  @Default
  HttpMethod method = GET;

  String url;

  @Default
  Map<String, List<String>> queryParams = emptyMap();

  @Default
  Map<String, List<String>> headers = emptyMap();

  byte[] body;

  public enum HttpMethod {
    GET, HEAD, POST, PUT, PATCH
  }
}

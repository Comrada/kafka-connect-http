package io.github.comrada.kafka.connect.http.response.transform;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.response.transform.spi.HttpResponseTransformer;

public class NoneTransformer implements HttpResponseTransformer {

  @Override
  public HttpResponse transform(HttpResponse response) {
    return response;
  }
}

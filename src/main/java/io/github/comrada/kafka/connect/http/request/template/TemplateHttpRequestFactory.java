package io.github.comrada.kafka.connect.http.request.template;

import static io.github.comrada.kafka.connect.common.ConfigUtils.breakDownHeaders;
import static io.github.comrada.kafka.connect.common.ConfigUtils.breakDownQueryParams;

import io.github.comrada.kafka.connect.http.model.HttpRequest;
import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.request.spi.HttpRequestFactory;
import io.github.comrada.kafka.connect.http.request.template.spi.Template;
import io.github.comrada.kafka.connect.http.request.template.spi.TemplateFactory;
import java.util.Map;

public class TemplateHttpRequestFactory implements HttpRequestFactory {

  private String method;
  private Template urlTpl;
  private Template headersTpl;
  private Template queryParamsTpl;
  private Template bodyTpl;

  @Override
  public void configure(Map<String, ?> configs) {
    final TemplateHttpRequestFactoryConfig config = new TemplateHttpRequestFactoryConfig(configs);
    final TemplateFactory templateFactory = config.getTemplateFactory();
    this.method = config.getMethod();
    this.urlTpl = templateFactory.create(config.getUrl());
    this.headersTpl = templateFactory.create(config.getHeaders());
    this.queryParamsTpl = templateFactory.create(config.getQueryParams());
    this.bodyTpl = templateFactory.create(config.getBody());
  }

  @Override
  public HttpRequest createRequest(Offset offset) {
    return HttpRequest.builder()
        .method(HttpRequest.HttpMethod.valueOf(method))
        .url(urlTpl.apply(offset))
        .headers(breakDownHeaders(headersTpl.apply(offset)))
        .queryParams(breakDownQueryParams(queryParamsTpl.apply(offset)))
        .body(bodyTpl.apply(offset).getBytes())
        .build();
  }
}

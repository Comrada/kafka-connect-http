package io.github.comrada.kafka.connect.http.request.template;

import io.github.comrada.kafka.connect.http.request.template.spi.Template;
import io.github.comrada.kafka.connect.http.request.template.spi.TemplateFactory;

public class NoTemplateFactory implements TemplateFactory {

  @Override
  public Template create(String template) {
    return offset -> template;
  }
}

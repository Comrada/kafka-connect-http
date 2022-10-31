package io.github.comrada.kafka.connect.http.request.template.freemarker;

import static java.util.UUID.randomUUID;

import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.request.template.spi.Template;
import io.github.comrada.kafka.connect.http.request.template.spi.TemplateFactory;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import freemarker.template.Version;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;

@Deprecated
public class BackwardsCompatibleFreeMarkerTemplateFactory implements TemplateFactory {

  private final Configuration configuration = new Configuration(new Version(2, 3, 30)) {{
    setNumberFormat("computer");
  }};

  @Override
  public Template create(String template) {
    return offset -> apply(createTemplate(template), createModel(offset));
  }

  @SneakyThrows(IOException.class)
  private freemarker.template.Template createTemplate(String template) {
    return new freemarker.template.Template(randomUUID().toString(), new StringReader(template), configuration);
  }

  private static Map<String, Object> createModel(Offset offset) {
    Map<String, Object> model = new HashMap<>(offset.toMap());
    model.put("offset", offset.toMap());
    return model;
  }

  @SneakyThrows({TemplateException.class, IOException.class})
  private String apply(freemarker.template.Template template, Map<String, Object> model) {
    Writer writer = new StringWriter();
    template.process(model, writer);
    return writer.toString();
  }
}

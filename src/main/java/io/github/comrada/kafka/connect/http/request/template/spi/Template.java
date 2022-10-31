package io.github.comrada.kafka.connect.http.request.template.spi;

import io.github.comrada.kafka.connect.http.model.Offset;

@FunctionalInterface
public interface Template {

  String apply(Offset offset);
}

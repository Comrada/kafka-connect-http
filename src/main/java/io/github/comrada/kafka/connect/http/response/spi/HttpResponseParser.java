package io.github.comrada.kafka.connect.http.response.spi;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.source.SourceRecord;

@FunctionalInterface
public interface HttpResponseParser extends Configurable {

  List<SourceRecord> parse(HttpResponse response);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

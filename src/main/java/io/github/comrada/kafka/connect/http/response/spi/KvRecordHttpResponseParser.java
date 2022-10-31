package io.github.comrada.kafka.connect.http.response.spi;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@FunctionalInterface
public interface KvRecordHttpResponseParser extends Configurable {

  List<KvRecord> parse(HttpResponse response);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

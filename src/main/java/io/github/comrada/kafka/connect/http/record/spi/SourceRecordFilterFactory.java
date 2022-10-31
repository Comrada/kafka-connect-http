package io.github.comrada.kafka.connect.http.record.spi;

import io.github.comrada.kafka.connect.http.model.Offset;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.source.SourceRecord;

@FunctionalInterface
public interface SourceRecordFilterFactory extends Configurable {

  Predicate<SourceRecord> create(Offset offset);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

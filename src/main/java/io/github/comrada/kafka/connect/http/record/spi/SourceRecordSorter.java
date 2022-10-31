package io.github.comrada.kafka.connect.http.record.spi;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.source.SourceRecord;

@FunctionalInterface
public interface SourceRecordSorter extends Configurable {

  List<SourceRecord> sort(List<SourceRecord> records);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

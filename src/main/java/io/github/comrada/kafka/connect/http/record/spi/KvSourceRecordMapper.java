package io.github.comrada.kafka.connect.http.record.spi;

import io.github.comrada.kafka.connect.http.record.model.KvRecord;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.source.SourceRecord;

@FunctionalInterface
public interface KvSourceRecordMapper extends Configurable {

  SourceRecord map(KvRecord record);

  default void configure(Map<String, ?> map) {
    // Do nothing
  }
}

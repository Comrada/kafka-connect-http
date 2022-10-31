package io.github.comrada.kafka.connect.http.record;

import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import java.util.function.Predicate;
import org.apache.kafka.connect.source.SourceRecord;

public class PassthroughRecordFilterFactory implements SourceRecordFilterFactory {

  @Override
  public Predicate<SourceRecord> create(Offset offset) {
    return __ -> true;
  }
}

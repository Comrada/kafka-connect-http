package io.github.comrada.kafka.connect.http.record;

import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import java.time.Instant;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.connect.source.SourceRecord;

@RequiredArgsConstructor
public class OffsetTimestampRecordFilterFactory implements SourceRecordFilterFactory {

  @Override
  public Predicate<SourceRecord> create(Offset offset) {
    Long offsetTimestampMillis = offset.getTimestamp().map(Instant::toEpochMilli).orElse(0L);
    return record -> record.timestamp() > offsetTimestampMillis;
  }
}

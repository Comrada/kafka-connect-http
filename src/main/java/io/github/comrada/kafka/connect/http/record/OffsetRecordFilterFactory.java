package io.github.comrada.kafka.connect.http.record;

import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.connect.source.SourceRecord;

@RequiredArgsConstructor
public class OffsetRecordFilterFactory implements SourceRecordFilterFactory {

  private final SourceRecordFilterFactory delegate;

  public OffsetRecordFilterFactory() {
    this(new OffsetTimestampRecordFilterFactory());
  }

  @Override
  public Predicate<SourceRecord> create(Offset offset) {
    AtomicBoolean lastSeenReached = new AtomicBoolean(false);
    return delegate.create(offset).or(record -> {
      boolean result = lastSeenReached.get();
      if (!result && Offset.of(record.sourceOffset()).equals(offset)) {
        lastSeenReached.set(true);
      }
      return result;
    });
  }
}

package io.github.comrada.kafka.connect.http.record;

import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection.ASC;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection.DESC;
import static java.util.Collections.reverse;
import static java.util.Objects.requireNonNull;

import io.github.comrada.kafka.connect.http.record.spi.SourceRecordSorter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.source.SourceRecord;

public class OrderDirectionSourceRecordSorter implements SourceRecordSorter {

  private final Function<Map<String, ?>, OrderDirectionSourceRecordSorterConfig> configFactory;

  private OrderDirection orderDirection;

  OrderDirectionSourceRecordSorter(Function<Map<String, ?>, OrderDirectionSourceRecordSorterConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public OrderDirectionSourceRecordSorter() {
    this(OrderDirectionSourceRecordSorterConfig::new);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    orderDirection = configFactory.apply(settings).getOrderDirection();
  }

  @Override
  public List<SourceRecord> sort(List<SourceRecord> records) {
    return sortWithDirection(records, orderDirection);
  }

  private List<SourceRecord> sortWithDirection(List<SourceRecord> records, OrderDirection direction) {
    switch (direction) {
      case DESC:
        List<SourceRecord> reversed = new ArrayList<>(records);
        reverse(reversed);
        return reversed;
      case ASC:
        return records;
      case IMPLICIT:
      default:
        return sortWithDirection(records, getImplicitDirection(records));
    }
  }

  private OrderDirection getImplicitDirection(List<SourceRecord> records) {
    if (records.size() >= 2) {
      Long first = records.get(0).timestamp();
      Long last = records.get(records.size() - 1).timestamp();
      return first <= last ? ASC : DESC;
    }
    return ASC;
  }

  public enum OrderDirection {
    ASC, DESC, IMPLICIT
  }
}

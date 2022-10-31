package io.github.comrada.kafka.connect.http.record;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class OrderDirectionSourceRecordSorterConfig extends AbstractConfig {

  private static final String ORDER_DIRECTION = "http.response.list.order.direction";

  private final OrderDirection orderDirection;

  OrderDirectionSourceRecordSorterConfig(Map<String, ?> originals) {
    super(config(), originals);
    orderDirection = OrderDirection.valueOf(getString(ORDER_DIRECTION).toUpperCase());
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(ORDER_DIRECTION, STRING, "IMPLICIT", LOW,
            "Order direction of the results in the list, either ASC, DESC or IMPLICIT");
  }
}

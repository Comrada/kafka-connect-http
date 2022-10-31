package io.github.comrada.kafka.connect.http.record;

import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection.ASC;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection.IMPLICIT;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorterConfigTest.Fixture.config;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OrderDirectionSourceRecordSorterConfigTest {

  @Test
  void whenNoDirection_thenDefault() {
    assertThat(config(emptyMap()).getOrderDirection()).isEqualTo(IMPLICIT);
  }

  @Test
  void whenDirection_thenInitialized() {
    assertThat(config(ImmutableMap.of("http.response.list.order.direction", "ASC")).getOrderDirection()).isEqualTo(ASC);
  }

  interface Fixture {

    static OrderDirectionSourceRecordSorterConfig config(Map<String, String> settings) {
      return new OrderDirectionSourceRecordSorterConfig(settings);
    }
  }

}

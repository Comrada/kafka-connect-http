package io.github.comrada.kafka.connect.http.record;

import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection.ASC;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection.DESC;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection.IMPLICIT;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorterTest.Fixture.mid;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorterTest.Fixture.newer;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorterTest.Fixture.older;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorterTest.Fixture.ordered;
import static io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorterTest.Fixture.reverseOrdered;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter.OrderDirection;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OrderDirectionSourceRecordSorterTest {

  OrderDirectionSourceRecordSorter sorter;

  @Mock
  OrderDirectionSourceRecordSorterConfig config;

  @Test
  void givenAsc_whenOrderedRecords_thenAsIs() {

    givenDirection(ASC);

    assertThat(sorter.sort(ordered)).containsExactly(older, mid, newer);
  }

  @Test
  void givenAsc_whenReverseOrderedRecords_thenAsIs() {

    givenDirection(ASC);

    assertThat(sorter.sort(reverseOrdered)).containsExactly(newer, mid, older);
  }

  @Test
  void givenDesc_whenOrderedRecords_thenReversed() {

    givenDirection(DESC);

    assertThat(sorter.sort(ordered)).containsExactly(newer, mid, older);
  }

  @Test
  void givenDesc_whenReverseOrderedRecords_thenReversed() {

    givenDirection(DESC);

    assertThat(sorter.sort(reverseOrdered)).containsExactly(older, mid, newer);
  }

  @Test
  void givenImplicit_whenOrderedRecords_thenAsIs() {

    givenDirection(IMPLICIT);

    assertThat(sorter.sort(ordered)).containsExactly(older, mid, newer);
  }

  @Test
  void givenImplicit_whenReverseOrderedRecords_thenAsIs() {

    givenDirection(IMPLICIT);

    assertThat(sorter.sort(reverseOrdered)).containsExactly(older, mid, newer);
  }

  private void givenDirection(OrderDirection asc) {
    sorter = new OrderDirectionSourceRecordSorter(__ -> config);
    given(config.getOrderDirection()).willReturn(asc);
    sorter.configure(Collections.emptyMap());
  }

  interface Fixture {

    SourceRecord older = new SourceRecord(null, null, null, null, null, null, null, null, MIN_VALUE);
    SourceRecord mid = new SourceRecord(null, null, null, null, null, null, null, null, 0L);
    SourceRecord newer = new SourceRecord(null, null, null, null, null, null, null, null, MAX_VALUE);
    List<SourceRecord> ordered = asList(older, mid, newer);
    List<SourceRecord> reverseOrdered = asList(newer, mid, older);
  }
}

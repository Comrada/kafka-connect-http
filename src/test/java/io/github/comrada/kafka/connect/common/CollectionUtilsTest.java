package io.github.comrada.kafka.connect.common;

import static io.github.comrada.kafka.connect.common.CollectionUtils.merge;
import static io.github.comrada.kafka.connect.common.CollectionUtilsTest.Fixture.map1;
import static io.github.comrada.kafka.connect.common.CollectionUtilsTest.Fixture.map1and3;
import static io.github.comrada.kafka.connect.common.CollectionUtilsTest.Fixture.map2;
import static io.github.comrada.kafka.connect.common.CollectionUtilsTest.Fixture.map3;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class CollectionUtilsTest {

  @Test
  void whenMergeMapWithEmpty_thenMap() {
    assertThat(merge(map1, emptyMap())).isEqualTo(map1);
  }

  @Test
  void whenMergeEmptyWithMap_thenMap() {
    assertThat(merge(emptyMap(), map1)).isEqualTo(map1);
  }

  @Test
  void whenMergeMapSameKey_thenLastValueWins() {
    assertThat(merge(map1, map2)).isEqualTo(map2);
  }

  @Test
  void whenMergeMapDiffKey_thenCombined() {
    assertThat(merge(map1, map3)).isEqualTo(map1and3);
  }

  interface Fixture {

    ImmutableMap<String, String> map1 = ImmutableMap.of("k1", "v1");
    ImmutableMap<String, String> map2 = ImmutableMap.of("k1", "v2");
    ImmutableMap<String, String> map3 = ImmutableMap.of("k2", "v3");
    ImmutableMap<String, String> map1and3 = ImmutableMap.of("k1", "v1", "k2", "v3");
  }
}

package io.github.comrada.kafka.connect.common;

import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CollectionUtils {

  public static <T, K, U> Collector<T, ?, LinkedHashMap<K, U>> toLinkedHashMap(
      Function<? super T, ? extends K> keyMapper,
      Function<? super T, ? extends U> valueMapper) {

    return toMap(
        keyMapper,
        valueMapper,
        (u, v) -> {
          throw new IllegalStateException(String.format("Duplicate key %s", u));
        },
        LinkedHashMap::new
    );
  }

  public static <S, T> Map<S, T> merge(Map<S, T> mapA, Map<S, T> mapB) {
    Map<S, T> merged = new HashMap<>(mapA);
    mapB.forEach((key, value) -> merged.merge(key, value, (k, v) -> v));
    return merged;
  }
}

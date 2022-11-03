package io.github.comrada.kafka.connect.http.ack;

import static io.github.comrada.kafka.connect.common.CollectionUtils.toLinkedHashMap;
import static java.util.function.Function.identity;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfirmationWindow<T> {

  private final LinkedHashMap<T, Boolean> confirmedOffsets;

  public ConfirmationWindow(List<T> offsets) {
    confirmedOffsets = offsets.stream().collect(toLinkedHashMap(identity(), __ -> false));
  }

  public void confirm(T offset) {
    confirmedOffsets.replace(offset, true);
    log.debug("Confirmed offset {}", offset);
  }

  public Optional<T> getLowWatermarkOffset() {
    T offset = null;
    for (Map.Entry<T, Boolean> offsetEntry : confirmedOffsets.entrySet()) {
      Boolean offsetWasConfirmed = offsetEntry.getValue();
      T sourceOffset = offsetEntry.getKey();
      if (offsetWasConfirmed) {
        offset = sourceOffset;
      } else {
        log.warn("Found unconfirmed offset {}. Will resume polling from previous offset. " +
            "This might result in a number of duplicated records.", sourceOffset);
        break;
      }
    }
    return Optional.ofNullable(offset);
  }
}

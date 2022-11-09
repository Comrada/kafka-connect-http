package io.github.comrada.kafka.connect;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public final class LogsCollector extends AppenderBase<ILoggingEvent> {

  private static final List<ILoggingEvent> EVENTS = new CopyOnWriteArrayList<>();

  @Override
  protected void append(ILoggingEvent e) {
    EVENTS.add(e);
  }

  public static void reset() {
    EVENTS.clear();
  }

  public static Optional<ILoggingEvent> findEventWithText(String text) {
    return EVENTS.stream().filter(event -> event.getFormattedMessage().contains(text)).findFirst();
  }
}

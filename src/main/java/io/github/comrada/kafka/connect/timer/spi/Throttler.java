package io.github.comrada.kafka.connect.timer.spi;

@FunctionalInterface
public interface Throttler {

  void throttle() throws InterruptedException;
}

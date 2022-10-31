package io.github.comrada.kafka.connect.timer.spi;

@FunctionalInterface
public interface Sleeper {

  void sleep(Long milliseconds) throws InterruptedException;
}

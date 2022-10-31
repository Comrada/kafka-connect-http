package io.github.comrada.kafka.connect.http;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

import io.github.comrada.kafka.connect.common.VersionUtils;
import io.github.comrada.kafka.connect.http.ack.ConfirmationWindow;
import io.github.comrada.kafka.connect.http.client.spi.HttpClient;
import io.github.comrada.kafka.connect.http.model.HttpRequest;
import io.github.comrada.kafka.connect.http.model.HttpResponse;
import io.github.comrada.kafka.connect.http.model.Offset;
import io.github.comrada.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import io.github.comrada.kafka.connect.http.record.spi.SourceRecordSorter;
import io.github.comrada.kafka.connect.http.request.spi.HttpRequestFactory;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponseParser;
import io.github.comrada.kafka.connect.timer.TimerThrottler;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

@Slf4j
public class HttpSourceTask extends SourceTask {

  private final Function<Map<String, String>, HttpSourceConnectorConfig> configFactory;
  protected TimerThrottler throttler;
  protected HttpRequestFactory requestFactory;
  protected HttpClient requestExecutor;
  protected HttpResponseParser responseParser;
  protected SourceRecordSorter recordSorter;
  protected SourceRecordFilterFactory recordFilterFactory;
  protected ConfirmationWindow<Map<String, ?>> confirmationWindow = new ConfirmationWindow<>(emptyList());
  @Getter
  private Offset offset;

  HttpSourceTask(Function<Map<String, String>, HttpSourceConnectorConfig> configFactory) {
    this.configFactory = requireNonNull(configFactory);
  }

  public HttpSourceTask() {
    this(HttpSourceConnectorConfig::new);
  }

  @Override
  public void start(Map<String, String> settings) {
    HttpSourceConnectorConfig config = configFactory.apply(settings);
    throttler = config.getThrottler();
    requestFactory = config.getRequestFactory();
    requestExecutor = config.getClient();
    responseParser = config.getResponseParser();
    recordSorter = config.getRecordSorter();
    recordFilterFactory = config.getRecordFilterFactory();
    offset = loadOffset(config.getInitialOffset());
  }

  private Offset loadOffset(Map<String, String> initialOffset) {
    Map<String, Object> restoredOffset = Optional.ofNullable(context.offsetStorageReader().offset(emptyMap()))
        .orElseGet(Collections::emptyMap);
    return Offset.of(!restoredOffset.isEmpty() ? restoredOffset : initialOffset);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    throttler.throttle(offset.getTimestamp().orElseGet(Instant::now));
    HttpRequest request = requestFactory.createRequest(offset);
    HttpResponse response = execute(request);
    List<SourceRecord> records = responseParser.parse(response);
    List<SourceRecord> unseenRecords = recordSorter.sort(records).stream()
        .filter(recordFilterFactory.create(offset))
        .collect(toUnmodifiableList());
    log.info("Request for offset {} yields {}/{} new records", offset.toMap(), unseenRecords.size(), records.size());
    confirmationWindow = new ConfirmationWindow<>(extractOffsets(unseenRecords));
    return unseenRecords;
  }

  protected HttpResponse execute(HttpRequest request) {
    try {
      return requestExecutor.execute(request);
    } catch (IOException e) {
      throw new RetriableException(e);
    }
  }

  protected List<Map<String, ?>> extractOffsets(List<SourceRecord> recordsToSend) {
    return recordsToSend.stream()
        .map(SourceRecord::sourceOffset)
        .collect(toUnmodifiableList());
  }

  @Override
  public void commitRecord(SourceRecord record, RecordMetadata metadata) {
    confirmationWindow.confirm(record.sourceOffset());
  }

  @Override
  public void commit() {
    offset = confirmationWindow.getLowWatermarkOffset()
        .map(Offset::of)
        .orElse(offset);
    log.debug("Offset set to {}", offset);
  }

  @Override
  public void stop() {
    // Nothing to do, no resources to release
  }

  @Override
  public String version() {
    return VersionUtils.getVersion();
  }
}

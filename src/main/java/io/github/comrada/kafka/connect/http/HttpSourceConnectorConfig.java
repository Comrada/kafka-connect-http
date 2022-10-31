package io.github.comrada.kafka.connect.http;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import io.github.comrada.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.comrada.kafka.connect.http.client.spi.HttpClient;
import io.github.comrada.kafka.connect.http.record.OffsetRecordFilterFactory;
import io.github.comrada.kafka.connect.http.record.OrderDirectionSourceRecordSorter;
import io.github.comrada.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import io.github.comrada.kafka.connect.http.record.spi.SourceRecordSorter;
import io.github.comrada.kafka.connect.http.request.spi.HttpRequestFactory;
import io.github.comrada.kafka.connect.http.request.template.TemplateHttpRequestFactory;
import io.github.comrada.kafka.connect.http.response.PolicyHttpResponseParser;
import io.github.comrada.kafka.connect.http.response.spi.HttpResponseParser;
import io.github.comrada.kafka.connect.timer.AdaptableIntervalTimer;
import io.github.comrada.kafka.connect.timer.TimerThrottler;
import io.github.comrada.kafka.connect.timer.spi.Timer;
import io.github.comrada.kafka.connect.common.ConfigUtils;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
class HttpSourceConnectorConfig extends AbstractConfig {

  private static final String TIMER = "http.timer";
  private static final String CLIENT = "http.client";
  private static final String REQUEST_FACTORY = "http.request.factory";
  private static final String RESPONSE_PARSER = "http.response.parser";
  private static final String RECORD_SORTER = "http.record.sorter";
  private static final String RECORD_FILTER_FACTORY = "http.record.filter.factory";
  private static final String OFFSET_INITIAL = "http.offset.initial";

  private final TimerThrottler throttler;
  private final HttpRequestFactory requestFactory;
  private final HttpClient client;
  private final HttpResponseParser responseParser;
  private final SourceRecordFilterFactory recordFilterFactory;
  private final SourceRecordSorter recordSorter;
  private final Map<String, String> initialOffset;

  HttpSourceConnectorConfig(Map<String, ?> originals) {
    super(config(), originals);
    Timer timer = getConfiguredInstance(TIMER, Timer.class);
    throttler = new TimerThrottler(timer);
    requestFactory = getConfiguredInstance(REQUEST_FACTORY, HttpRequestFactory.class);
    client = getConfiguredInstance(CLIENT, HttpClient.class);
    responseParser = getConfiguredInstance(RESPONSE_PARSER, HttpResponseParser.class);
    recordSorter = getConfiguredInstance(RECORD_SORTER, SourceRecordSorter.class);
    recordFilterFactory = getConfiguredInstance(RECORD_FILTER_FACTORY, SourceRecordFilterFactory.class);
    initialOffset = ConfigUtils.breakDownMap(getString(OFFSET_INITIAL));
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(TIMER, CLASS, AdaptableIntervalTimer.class, HIGH, "Poll Timer Class")
        .define(CLIENT, CLASS, OkHttpClient.class, HIGH, "Request Client Class")
        .define(REQUEST_FACTORY, CLASS, TemplateHttpRequestFactory.class, HIGH, "Request Factory Class")
        .define(RESPONSE_PARSER, CLASS, PolicyHttpResponseParser.class, HIGH, "Response Parser Class")
        .define(RECORD_SORTER, CLASS, OrderDirectionSourceRecordSorter.class, LOW, "Record Sorter Class")
        .define(RECORD_FILTER_FACTORY, CLASS, OffsetRecordFilterFactory.class, LOW, "Record Filter Factory Class")
        .define(OFFSET_INITIAL, STRING, "", HIGH, "Starting offset");
  }
}

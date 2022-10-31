package io.github.comrada.kafka.connect.http.response.jackson;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;

import io.github.comrada.kafka.connect.http.response.timestamp.EpochMillisOrDelegateTimestampParser;
import io.github.comrada.kafka.connect.http.response.timestamp.spi.TimestampParser;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class JacksonKvRecordHttpResponseParserConfig extends AbstractConfig {

  private static final String RECORD_TIMESTAMP_PARSER_CLASS = "http.response.record.timestamp.parser";

  private final JacksonResponseRecordParser responseParser;
  private final TimestampParser timestampParser;

  JacksonKvRecordHttpResponseParserConfig(Map<String, ?> originals) {
    super(config(), originals);
    JacksonSerializer serializer = new JacksonSerializer();
    JacksonRecordParser recordParser = new JacksonRecordParser(serializer);
    recordParser.configure(originals);
    this.responseParser = new JacksonResponseRecordParser(recordParser, serializer);
    this.responseParser.configure(originals);
    this.timestampParser = getConfiguredInstance(RECORD_TIMESTAMP_PARSER_CLASS, TimestampParser.class);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(RECORD_TIMESTAMP_PARSER_CLASS, CLASS, EpochMillisOrDelegateTimestampParser.class, LOW,
            "Record Timestamp parser class");
  }
}

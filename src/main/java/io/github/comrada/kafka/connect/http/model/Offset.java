package io.github.comrada.kafka.connect.http.model;

import static java.util.Optional.ofNullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class Offset {

  private static final String KEY_KEY = "key";

  private static final String TIMESTAMP_KEY = "timestamp";

  private final Map<String, ?> properties;

  private Offset(Map<String, ?> properties) {
    this.properties = properties;
  }

  public static Offset of(Map<String, ?> properties) {
    return new Offset(properties);
  }

  public static Offset of(Map<String, ?> properties, String key) {
    Map<String, Object> props = new HashMap<>(properties);
    props.put(KEY_KEY, key);
    return new Offset(props);
  }

  public static Offset of(Map<String, ?> properties, String key, Instant timestamp) {
    Map<String, Object> props = new HashMap<>(properties);
    props.put(KEY_KEY, key);
    props.put(TIMESTAMP_KEY, timestamp.toString());
    return new Offset(props);
  }

  public Map<String, ?> toMap() {
    return properties;
  }

  public Optional<String> getKey() {
    return ofNullable((String) properties.get(KEY_KEY));
  }

  public Optional<Instant> getTimestamp() {
    return ofNullable((String) properties.get(TIMESTAMP_KEY)).map(Instant::parse);
  }
}

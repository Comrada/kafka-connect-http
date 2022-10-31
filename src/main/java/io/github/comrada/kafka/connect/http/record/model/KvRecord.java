package io.github.comrada.kafka.connect.http.record.model;

import io.github.comrada.kafka.connect.http.model.Offset;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@With
@Value
@Builder
public class KvRecord {

  String key;
  String value;
  Offset offset;
}

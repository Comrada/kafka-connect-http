package io.github.comrada.kafka.connect;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.comrada.kafka.connect.http.HttpSourceTask;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

public final class TestUtils {

  private TestUtils() {
  }

  public static String loadResourceFile(Class<?> loader, String fileName) throws IOException {
    try (InputStream inputStream = loader.getResourceAsStream(fileName)) {
      return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  public static void mockTaskContext(HttpSourceTask task) {
    SourceTaskContext taskContext = mock(SourceTaskContext.class);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    when(offsetStorageReader.offset(any())).thenReturn(null);
    when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
    try {
      Field contextField = task.getClass().getSuperclass().getDeclaredField("context");
      contextField.setAccessible(true);
      contextField.set(task, taskContext);
      contextField.setAccessible(false);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static String extractStringValue(SourceRecord sourceRecord) {
    Object recordValue = sourceRecord.value();
    if (recordValue instanceof Struct) {
      Struct structuredRecord = (Struct) recordValue;
      Object value = structuredRecord.get("value");
      if (value instanceof String) {
        return (String) value;
      } else {
        String errorMessage = "Unknown value type: " + value.getClass();
        throw new IllegalArgumentException(errorMessage);
      }
    } else {
      String errorMessage = "Unknown record type: " + recordValue.getClass();
      throw new IllegalArgumentException(errorMessage);
    }
  }
}

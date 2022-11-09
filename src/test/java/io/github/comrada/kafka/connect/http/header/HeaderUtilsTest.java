package io.github.comrada.kafka.connect.http.header;

import static io.github.comrada.kafka.connect.LogsCollector.findEventWithText;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.comrada.kafka.connect.LogsCollector;
import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class HeaderUtilsTest {

  @BeforeAll
  static void resetLogsBefore() {
    LogsCollector.reset();
  }

  @AfterAll
  static void resetLogsAfter() {
    LogsCollector.reset();
  }

  @ParameterizedTest
  @CsvSource({
      "application/json; charset=UTF-8,UTF-8",
      "application/json;charset=UTF-8,UTF-8",
      "application/json,UTF-8",
      "text/html; charset=utf-8,UTF-8"
  })
  void getCharset(String header, String charsetName) {
    HttpResponse response = HttpResponse.builder()
        .headers(Map.of("Content-Type", singletonList(header)))
        .build();

    assertEquals(Charset.forName(charsetName), HeaderUtils.getCharset(response));
  }

  @Test
  void whenResponseHasUnsupportedCharset_thenDefaultIsUsed() {
    HttpResponse response = HttpResponse.builder()
        .headers(Map.of("Content-Type", singletonList("application/json;charset=FAKE-CHARSET")))
        .build();

    assertEquals(StandardCharsets.UTF_8, HeaderUtils.getCharset(response));
    assertTrue(findEventWithText("HTTP response has unsupported charset: FAKE-CHARSET, UTF-8 is used instead").isPresent());
  }

  @ParameterizedTest
  @CsvSource({
      "Content-Type,application/json;charset=UTF-8",
      "Content-type,text/html; charset=utf-8",
      "content-type,text/html; charset=utf-8"
  })
  void getContentTypeHeader(String header, String headerValue) {
    HttpResponse response = HttpResponse.builder()
        .headers(Map.of(header, singletonList(headerValue)))
        .build();
    Optional<String> actual = HeaderUtils.getContentTypeHeader(response);

    assertTrue(actual.isPresent());
    assertEquals(headerValue, actual.get());
  }

  @ParameterizedTest
  @CsvSource({
      "Content-Type,application/json;charset=UTF-8,application/json",
      "Content-type,text/html; charset=utf-8,text/html",
      "content-type,text/html; charset=utf-8,text/html"
  })
  void getContentType(String header, String headerValue, String contentType) {
    HttpResponse response = HttpResponse.builder()
        .headers(Map.of(header, singletonList(headerValue)))
        .build();
    Optional<String> actual = HeaderUtils.getContentType(response);

    assertTrue(actual.isPresent());
    assertEquals(contentType, actual.get());
  }
}
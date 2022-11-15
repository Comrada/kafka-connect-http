package io.github.comrada.kafka.connect.http.header;

import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class HeaderUtils {

  private static final String CONTENT_TYPE_HEADER = "content-type";
  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private static final Pattern CHARSET_PARAM = Pattern.compile("charset=([-\\w]+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern CONTENT_TYPE_VALUE = Pattern.compile("[\\w-]+/[\\w-]+", Pattern.CASE_INSENSITIVE);

  private HeaderUtils() {
  }

  public static Charset getCharset(HttpResponse response) {
    return getContentTypeHeader(response)
        .map(contentType -> {
          Matcher matcher = CHARSET_PARAM.matcher(contentType);
          if (matcher.find()) {
            String charsetName = matcher.group(1).toUpperCase();
            try {
              return Charset.forName(charsetName);
            } catch (UnsupportedCharsetException e) {
              log.warn("HTTP response has unsupported charset: {}, {} is used instead", charsetName, DEFAULT_CHARSET);
            }
          }
          return DEFAULT_CHARSET;
        })
        .orElse(DEFAULT_CHARSET);
  }

  public static Optional<String> getContentTypeHeader(HttpResponse response) {
    return response.getHeaders().entrySet().stream()
        .filter(header -> header.getKey().equalsIgnoreCase(CONTENT_TYPE_HEADER))
        .map(Entry::getValue)
        .findFirst()
        .flatMap(contentTypes -> contentTypes.stream().findFirst());
  }

  public static Optional<String> getContentType(HttpResponse response) {
    Optional<String> header = getContentTypeHeader(response);
    if (header.isPresent()) {
      Matcher matcher = CONTENT_TYPE_VALUE.matcher(header.get());
      if (matcher.find()) {
        return Optional.of(matcher.group().toLowerCase());
      }
    }
    return Optional.empty();
  }
}

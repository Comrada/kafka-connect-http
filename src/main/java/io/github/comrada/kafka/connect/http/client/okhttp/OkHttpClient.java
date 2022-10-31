package io.github.comrada.kafka.connect.http.client.okhttp;

import static java.net.Proxy.NO_PROXY;
import static java.net.Proxy.Type.HTTP;
import static java.util.Optional.empty;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static okhttp3.Credentials.basic;
import static okhttp3.HttpUrl.parse;
import static okhttp3.RequestBody.create;
import static okhttp3.logging.HttpLoggingInterceptor.Level.BASIC;
import static okhttp3.logging.HttpLoggingInterceptor.Level.BODY;
import static okhttp3.logging.HttpLoggingInterceptor.Level.NONE;
import static org.apache.commons.lang.StringUtils.isEmpty;

import io.github.comrada.kafka.connect.http.auth.spi.HttpAuthenticator;
import io.github.comrada.kafka.connect.http.client.spi.HttpClient;
import io.github.comrada.kafka.connect.http.model.HttpRequest;
import io.github.comrada.kafka.connect.http.model.HttpResponse;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Authenticator;
import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.logging.HttpLoggingInterceptor;

@Slf4j
public class OkHttpClient implements HttpClient {

  private okhttp3.OkHttpClient client;

  private HttpAuthenticator authenticator;

  @Override
  public void configure(Map<String, ?> configs) {
    final OkHttpClientConfig config = new OkHttpClientConfig(configs);
    this.authenticator = config.getAuthenticator();
    var builder = new okhttp3.OkHttpClient.Builder()
        .connectionPool(new ConnectionPool(config.getMaxIdleConnections(), config.getKeepAliveDuration(), MILLISECONDS))
        .connectTimeout(config.getConnectionTimeoutMillis(), MILLISECONDS)
        .readTimeout(config.getReadTimeoutMillis(), MILLISECONDS)
        .retryOnConnectionFailure(true)
        .addInterceptor(createLoggingInterceptor())
        .addInterceptor(chain -> chain.proceed(authorize(chain.request())))
        .authenticator((route, response) -> authorize(response.request()))
        .proxy(resolveProxy(config.getProxyHost(), config.getProxyPort()))
        .proxyAuthenticator(resolveProxyAuthenticator(config.getProxyUsername(), config.getProxyPassword()));
    resolveSslSocketFactory(builder, config.getKeyStore(), config.getKeyStorePassword().value());
    this.client = builder.build();
  }

  private Request authorize(Request request) {
    return authenticator.getAuthorizationHeader()
        .map(header -> request.newBuilder().header("Authorization", header).build())
        .orElse(request);
  }

  private static HttpLoggingInterceptor createLoggingInterceptor() {
    if (log.isTraceEnabled()) {
      return new HttpLoggingInterceptor(log::trace).setLevel(BODY);
    } else if (log.isDebugEnabled()) {
      return new HttpLoggingInterceptor(log::debug).setLevel(BASIC);
    } else {
      return new HttpLoggingInterceptor(log::info).setLevel(NONE);
    }
  }

  private static Proxy resolveProxy(String host, Integer port) {
    return isEmpty(host) ? NO_PROXY : new Proxy(HTTP, new InetSocketAddress(host, port));
  }

  private static Authenticator resolveProxyAuthenticator(String username, String password) {
    return isEmpty(username) ? Authenticator.NONE :
        (route, response) -> response.request().newBuilder()
            .header("Proxy-Authorization", basic(username, password))
            .build();
  }

  private static void resolveSslSocketFactory(okhttp3.OkHttpClient.Builder builder, String keyStorePath,
      String keyStorePassword) {
    if (keyStorePath.isEmpty()) {
      return;
    }

    final KeyStore keyStore;
    try {
      keyStore = KeyStore.getInstance("PKCS12");
    } catch (KeyStoreException e) {
      throw new IllegalStateException("Unable to create keystore", e);
    }

    try (InputStream is = new FileInputStream(keyStorePath)) {
      keyStore.load(is, keyStorePassword.toCharArray());
    } catch (CertificateException | IOException | NoSuchAlgorithmException e) {
      throw new IllegalStateException(String.format("Unable to load keystore '%s'", keyStorePath), e);
    }

    KeyManagerFactory kmf;
    try {
      kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, keyStorePassword.toCharArray());
    } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new IllegalStateException("Unable to initialize key manager", e);
    }

    SSLContext sslContext;
    try {
      sslContext = SSLContext.getInstance("TLS");
      sslContext.init(kmf.getKeyManagers(), null, null);
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new IllegalStateException("Unable to initialize SSL context", e);
    }

    TrustManagerFactory trustManagerFactory;
    try {
      trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init((KeyStore) null);
    } catch (NoSuchAlgorithmException | KeyStoreException e) {
      throw new IllegalStateException("Unable to initialize trust manager", e);
    }

    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
      throw new IllegalStateException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
    }

    builder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustManagers[0]);
  }

  @Override
  @SneakyThrows(IOException.class)
  public HttpResponse execute(HttpRequest httpRequest) {
    Request request = mapHttpRequest(httpRequest);
    Call call = client.newCall(request);
    try (Response response = call.execute()) {
      return mapHttpResponse(response);
    }
  }

  private Request mapHttpRequest(HttpRequest request) {
    var builder = new Request.Builder();
    builder.url(mapUrl(request.getUrl(), request.getQueryParams()));
    addHeaders(builder, request);
    addMethodWithBody(builder, request);
    return builder.build();
  }

  private HttpUrl mapUrl(String url, Map<String, List<String>> queryParams) {
    HttpUrl httpUrl = parse(url);
    if (httpUrl == null) {
      throw new IllegalStateException(String.format("Illegal url: %s", url));
    }
    HttpUrl.Builder urlBuilder = httpUrl.newBuilder();
    queryParams.forEach((k, list) -> list.forEach(v -> urlBuilder.addEncodedQueryParameter(k, v)));
    return urlBuilder.build();
  }

  private void addHeaders(Request.Builder builder, HttpRequest request) {
    request.getHeaders()
        .forEach((name, values) -> values
            .forEach(value ->
                builder.addHeader(name, value)));
  }

  private void addMethodWithBody(Request.Builder builder, HttpRequest request) {
    switch (request.getMethod()) {
      case HEAD:
        builder.head();
        break;
      case PUT:
        mapBody(request).ifPresent(builder::put);
        break;
      case POST:
        mapBody(request).ifPresent(builder::post);
        break;
      case PATCH:
        mapBody(request).ifPresent(builder::patch);
        break;
      case GET:
      default:
        builder.get();
        break;
    }
  }

  private Optional<RequestBody> mapBody(HttpRequest request) {
    if (request.getBody() != null) {
      return Optional.of(create(request.getBody()));
    }
    return empty();
  }

  private HttpResponse mapHttpResponse(Response response) throws IOException {
    return HttpResponse.builder()
        .code(response.code())
        .body(response.body() != null ? response.body().bytes() : new byte[0])
        .headers(response.headers().toMultimap())
        .build();
  }
}

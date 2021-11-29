/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor;

import static azkaban.Constants.ConfigurationKeys.EXECUTOR_CONNECTION_TLS_ENABLED;
import static com.google.common.base.Preconditions.checkState;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.RestfulApiClient;
import azkaban.utils.UndefinedPropertyException;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client class that will be used to handle all Restful API calls between Executor and the host
 * application.
 */
@Singleton
public class ExecutorApiClient extends RestfulApiClient<String> {

  private final static Logger logger = LoggerFactory.getLogger(ExecutorApiClient.class);
  private final static String DEFAULT_TRUSTSTORE_PATH = "keystore";
  private final static String DEFAULT_TRUSTSTORE_PASSWORD = "changeit";

  private final boolean isReverseProxyEnabled;
  private final Optional<String> reverseProxyHost;
  private final Optional<Integer> reverseProxyPort;
  private final boolean isTlsEnabled;
  private final String truststorePath;
  private final String truststorePassword;
  private SSLConnectionSocketFactory tlsSocketFactory;

  @Inject
  public ExecutorApiClient(final Props azkProps) {
    super();
    isReverseProxyEnabled =
        azkProps.getBoolean(ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_ENABLED,
        false);
    String reverseProxyHost = null;
    Integer reverseProxyPort = null;
    if (isReverseProxyEnabled){
      try {
        reverseProxyHost =
            azkProps.getString(ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_HOSTNAME);
      } catch (UndefinedPropertyException upe) {
        logger.error("Property {} must be specified when if the executor reverse proxy is enabled"
            + " {}.", ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_HOSTNAME,
            ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_ENABLED, upe);
        throw upe;
      }
      try {
        reverseProxyPort =
            azkProps.getInt(ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_PORT);
      } catch (UndefinedPropertyException upe) {
        logger.error("Property {} must be specified when if the executor reverse proxy is enabled"
                + " {}.", ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_PORT,
            ConfigurationKeys.AZKABAN_EXECUTOR_REVERSE_PROXY_ENABLED, upe);
        throw upe;
      }
    }
    this.reverseProxyHost = Optional.ofNullable(reverseProxyHost);
    this.reverseProxyPort = Optional.ofNullable(reverseProxyPort);

    this.isTlsEnabled = azkProps.getBoolean(EXECUTOR_CONNECTION_TLS_ENABLED, false);
    this.truststorePath = azkProps
        .getString(Constants.JETTY_TRUSTSTORE_PATH, this.DEFAULT_TRUSTSTORE_PATH);
    this.truststorePassword = azkProps.getString(Constants.JETTY_TRUSTSTORE_PASSWORD,
        this.DEFAULT_TRUSTSTORE_PASSWORD);
    if (this.isTlsEnabled) {
      setupTlsSocketFactory();
    }
  }

  /**
   * Build the URI, using the reverse-proxy settings if configured. Note that any changes to the
   * uri path required for reverse proxying, such as altering the path with execution-id, should
   * be applied to {@param path} before invoking this method.
   *
   * @param host host name.
   * @param port host port.
   * @param path extra path after host.
   * @param isHttp indicates if whether Http or HTTPS should be used.
   * @param params extra query parameters.
   * @return the URI built from the inputs.
   */
  public URI buildExecutorUri(final String host, final int port, final String path,
      final boolean isHttp, final Pair<String, String>... params) throws IOException {
    if (!isReverseProxyEnabled) {
      return  RestfulApiClient.buildUri(host, port, path, isHttp, params);
    }
    checkState(reverseProxyHost.isPresent());
    checkState(reverseProxyPort.isPresent());
    return RestfulApiClient.buildUri(reverseProxyHost.get(), reverseProxyPort.get(), path,
        isTlsEnabled, params);
  }

  private void setupTlsSocketFactory() {
    SSLContextBuilder sslContextBuilder = SSLContexts.custom();
    SSLContext sslContext = null;
    try {
      sslContextBuilder = sslContextBuilder
          .loadTrustMaterial(new File(this.truststorePath), this.truststorePassword.toCharArray());
      sslContext = sslContextBuilder.build();
    } catch (final NoSuchAlgorithmException | KeyStoreException |
        CertificateException | IOException | KeyManagementException ex) {
      logger.error("ExecutorApiClient could not be created due to exception: ", ex);
      throw new IllegalStateException("TLS context creation failed.", ex);
    }
    logger.debug("Creating SSLSocketFactory with hostname verification disabled");
    this.tlsSocketFactory = new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());
  }

  @VisibleForTesting
  SSLConnectionSocketFactory getTlsSocketFactory() {
    return this.tlsSocketFactory;
  }

  public boolean isReverseProxyEnabled() {
    return isReverseProxyEnabled;
  }

  @VisibleForTesting
  Optional<String> getReverseProxyHost() {
    return reverseProxyHost;
  }

  @VisibleForTesting
  Optional<Integer> getReverseProxyPort() {
    return reverseProxyPort;
  }

  /**
   * Overrides the parent implementation to provide a TLS enabled http client if requested,
   * otherwise returns the http client by invoking the parent class method.
   *
   * @return http client
   */
  @Override
  protected CloseableHttpClient createHttpClient() {
    if (!this.isTlsEnabled) {
      return super.createHttpClient();
    }
    final HttpClientBuilder httpClientBuilder = HttpClients.custom()
        .setSSLSocketFactory(this.tlsSocketFactory);
    return httpClientBuilder.build();
  }

  /**
   * Implementing the parseResponse function to return de-serialized Json object.
   *
   * @param response the returned response from the HttpClient.
   * @return de-serialized object from Json or null if the response doesn't have a body.
   */
  @Override
  protected String parseResponse(final HttpResponse response)
      throws HttpResponseException, IOException {
    final StatusLine statusLine = response.getStatusLine();
    final String responseBody = response.getEntity() != null ?
        EntityUtils.toString(response.getEntity()) : "";

    if (statusLine.getStatusCode() >= 300) {

      RestfulApiClient.logger
          .error(String.format("Unable to parse response as the response status is %s",
              statusLine.getStatusCode()));

      throw new HttpResponseException(statusLine.getStatusCode(), responseBody);
    }

    return responseBody;
  }
}

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

import azkaban.Constants;
import azkaban.utils.Props;
import azkaban.utils.RestfulApiClient;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

/**
 * Client class that will be used to handle all Restful API calls between Executor and the host
 * application.
 */
@Singleton
public class ExecutorApiClient extends RestfulApiClient<String> {

  final private static Logger logger = Logger.getLogger(ExecutorApiClient.class);

  final private String DEFAULT_TRUSTSTORE_PATH = "keystore";
  final private String DEFAULT_TRUSTSTORE_PASSWORD = "changeit";
  final private boolean isTlsEnabled;
  final private String truststorePath;
  final private String truststorePassword;
  private SSLConnectionSocketFactory tlsSocketFactory;

  @Inject
  public ExecutorApiClient(final Props azkProps) {
    this.isTlsEnabled = azkProps.getBoolean(EXECUTOR_CONNECTION_TLS_ENABLED, false);
    this.truststorePath = azkProps
        .getString(Constants.JETTY_TRUSTSTORE_PATH, this.DEFAULT_TRUSTSTORE_PATH);
    this.truststorePassword = azkProps.getString(Constants.JETTY_TRUSTSTORE_PASSWORD,
        this.DEFAULT_TRUSTSTORE_PASSWORD);
    if (this.isTlsEnabled) {
      setupTlsSocketFactory();
    }
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
    }
    logger.debug("Creating SSLSocketFactory with hostname verification disabled");
    this.tlsSocketFactory = new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());
  }

  @VisibleForTesting
  SSLConnectionSocketFactory getTlsSocketFactory() {
    return this.tlsSocketFactory;
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
          .error(String.format("unable to parse response as the response status is %s",
              statusLine.getStatusCode()));

      throw new HttpResponseException(statusLine.getStatusCode(), responseBody);
    }

    return responseBody;
  }
}

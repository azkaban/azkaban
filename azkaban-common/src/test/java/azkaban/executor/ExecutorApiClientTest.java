/*
 * Copyright 2020 LinkedIn Corp.
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
import static azkaban.Constants.JETTY_TRUSTSTORE_PASSWORD;
import static azkaban.Constants.JETTY_TRUSTSTORE_PATH;

import azkaban.utils.Props;
import java.io.IOException;
import java.net.URI;
import javax.net.ssl.SSLHandshakeException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

/**
 * For testing of ExecutorApiClient. This is currently focused on sanity testing TLS client side
 * support introduced within the ExecutorApiClient. The core functionality of ExecutorApiClient is
 * already expected to be tested through other means, such as through ExecutorApiGateway tests.
 * Highlights:
 * <p> 1. Launches a TLS enabled web server (https)
 * <p> 2. Creates ExecutorApiClient with TLS settings configured through properties
 * <p> 3. Verifies GET and POST requests against the TLS enabled server
 * <p> 4. Includes negative tests where the truststore certs don't match the private key
 * <p> 5. Uses pre-packaged keys and certs in a keystore and truststore.
 */
public class ExecutorApiClientTest {

  private static final int JETTY_TLS_PORT = 31311;
  private static final String TRUSTSTORE_PATH =
      ExecutorApiClient.class.getResource("test-cacerts").getPath();
  private static final String KEYSTORE_PATH =
      ExecutorApiClient.class.getResource("test-keystore").getPath();
  private static final String DEFAULT_PASSWORD = "changeit"; //for key, keystore and truststore
  private static Props tlsEnabledProps;
  private static Server tlsEnabledServer;

  // Commands used to create a self-signed certificate, build the test truststore and keystore.
  // $> openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 3650
  // $> openssl x509 -outform der -in cert.pem -out cert.der
  // $> keytool -import -alias truststore-test-cert -keystore test-cacerts -file cert.der
  //
  // $> openssl pkcs12 -export -out keystore.p12 -inkey key.pem -in cert.pem
  // $> keytool -importkeystore -srckeystore keystore.p12 -srcstoretype pkcs12 -destkeystore
  //  test-keystore -deststoretype jks -deststorepass changeit

  private static void setupTlsEnabledServer() throws Exception {
    final SslSocketConnector secureConnector = new SslSocketConnector();
    secureConnector.setPort(JETTY_TLS_PORT);
    secureConnector.setKeystore(KEYSTORE_PATH);
    secureConnector.setPassword(DEFAULT_PASSWORD);
    secureConnector.setKeyPassword(DEFAULT_PASSWORD);
    secureConnector.setTruststore(TRUSTSTORE_PATH);
    secureConnector.setTrustPassword(DEFAULT_PASSWORD);
    final QueuedThreadPool queuedThreadPool = new QueuedThreadPool(2);

    tlsEnabledServer = new Server();
    tlsEnabledServer.setThreadPool(queuedThreadPool);
    tlsEnabledServer.addConnector(secureConnector);

    final Context root = new Context(tlsEnabledServer, "/", Context.SESSIONS);
    root.addServlet(new ServletHolder(new SimpleServlet()), "/simple");

    tlsEnabledServer.start();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tlsEnabledProps = new Props();
    tlsEnabledProps.put(EXECUTOR_CONNECTION_TLS_ENABLED, "true");
    tlsEnabledProps
        .put(JETTY_TRUSTSTORE_PATH, ExecutorApiClient.class.getResource("test-cacerts").getPath());
    tlsEnabledProps.put(JETTY_TRUSTSTORE_PASSWORD, "changeit");
    setupTlsEnabledServer();
  }

  @Test
  public void testTlsEnabledApiClient() {
    final ExecutorApiClient tlsEnabledClient = new ExecutorApiClient(this.tlsEnabledProps);
    Assert.assertNotNull(tlsEnabledClient);
    Assert.assertNotNull(tlsEnabledClient.getTlsSocketFactory());
  }

  @Test
  public void testPostResponse() throws Exception {
    final ExecutorApiClient tlsEnabledClient = new ExecutorApiClient(this.tlsEnabledProps);
    final String postResponse = tlsEnabledClient
        .httpPost(new URI(SimpleServlet.TLS_ENABLED_URI), null);
    Assert.assertEquals(SimpleServlet.POST_RESPONSE_STRING, postResponse);
  }

  @Test
  public void testGetResponse() throws Exception {
    // Currently ExecutorApiClient does not make any POST requests.
    // This is for sanity testing that TLS enabled http-client continues working as expected with
    // GET requests as well.
    final ExecutorApiClient tlsEnabledClient = new ExecutorApiClient(this.tlsEnabledProps);
    final HttpClient httpClient = tlsEnabledClient.createHttpClient();
    final HttpGet httpGet = new HttpGet(SimpleServlet.TLS_ENABLED_URI);
    final HttpResponse httpResponse = httpClient.execute(httpGet);
    Assert.assertNotNull(httpResponse);

    final String getResponse = EntityUtils.toString(httpResponse.getEntity());
    Assert.assertEquals(SimpleServlet.GET_RESPONSE_STRING, getResponse);
  }

  @Test
  public void testCreateDefaultExecutorApiClient() {
    final ExecutorApiClient tlsDisabledClient = new ExecutorApiClient(new Props());
    Assert.assertNotNull(tlsDisabledClient);
    Assert.assertNull(tlsDisabledClient.getTlsSocketFactory());
  }

  @Test(expected = SSLHandshakeException.class)
  public void testFailureWithClientTlsDisabled() throws Exception {
    final ExecutorApiClient tlsDisabledClient = new ExecutorApiClient(new Props());
    // this should throw SSLHandshakeException
    final String postResponse = tlsDisabledClient
        .httpPost(new URI(SimpleServlet.TLS_ENABLED_URI), null);
    Assert.fail();
  }

  @Test(expected = SSLHandshakeException.class)
  public void testFailureWithInvalidCerts() throws Exception {
    final Props tlsPropsWithInvalidCert = new Props(tlsEnabledProps);
    tlsPropsWithInvalidCert.put(JETTY_TRUSTSTORE_PATH,
        ExecutorApiClient.class.getResource("invalid-cacerts").getPath());
    final ExecutorApiClient tlsEnabledClient = new ExecutorApiClient(tlsPropsWithInvalidCert);

    // this should throw SSLHandshakeException
    final String postResponse = tlsEnabledClient
        .httpPost(new URI(SimpleServlet.TLS_ENABLED_URI), null);
    Assert.fail();
  }

  private static class SimpleServlet extends HttpServlet {

    private static final long serialVersionUID = 1520347403053074355L;
    public static final String TLS_ENABLED_URI =
        "https://localhost:" + ExecutorApiClientTest.JETTY_TLS_PORT + "/simple";
    public static final String GET_RESPONSE_STRING = "{type: 'GET'}";
    public static final String POST_RESPONSE_STRING = "{type: 'POST'}";

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
        throws ServletException, IOException {
      resp.getWriter().print(this.GET_RESPONSE_STRING);
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp)
        throws ServletException, IOException {
      resp.getWriter().print(this.POST_RESPONSE_STRING);
    }
  }
}

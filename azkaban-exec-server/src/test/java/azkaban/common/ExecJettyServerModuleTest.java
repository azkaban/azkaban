/*
 * Copyright 2021 LinkedIn Corp.
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
 *
 */

package azkaban.common;

import static azkaban.Constants.ConfigurationKeys.EXECUTOR_CLIENT_TLS_ENABLED;
import static azkaban.Constants.ConfigurationKeys.EXECUTOR_CLIENT_TRUSTSTORE_PASSWORD;
import static azkaban.Constants.ConfigurationKeys.EXECUTOR_CLIENT_TRUSTSTORE_PATH;
import static azkaban.executor.ExecutorApiClientTest.DEFAULT_PASSWORD;
import static azkaban.executor.ExecutorApiClientTest.JETTY_PORT;
import static azkaban.executor.ExecutorApiClientTest.JETTY_TLS_PORT;
import static azkaban.executor.ExecutorApiClientTest.KEYSTORE_PATH;
import static azkaban.executor.ExecutorApiClientTest.TRUSTSTORE_PATH;

import azkaban.DispatchMethod;
import azkaban.executor.ExecutorApiClient;
import azkaban.executor.ExecutorApiClientTest.SimpleServlet;
import azkaban.utils.Props;
import java.net.URI;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;


/**
 * Test class for {@link ExecJettyServerModule}
 */
public class ExecJettyServerModuleTest {

  /**
   * This test tries to create the jetty-server and verify that it is reachable on the attached port
   * via Http.
   */
  @Test
  public void testSslDisabledJettyServer() throws Exception {
    ExecJettyServerModule execJettyServerModule = new ExecJettyServerModule();

    Props props = new Props();
    props.put("jetty.use.ssl", "false");
    props.put("executor.port", JETTY_PORT);
    Server jettyServer = execJettyServerModule.createJettyServer(props);
    Assert.assertEquals(1, jettyServer.getConnectors().length);
    Assert.assertTrue(jettyServer.getConnectors()[0] instanceof SocketConnector);
    SocketConnector socketConnector = (SocketConnector) jettyServer.getConnectors()[0];
    Assert.assertEquals(JETTY_PORT, socketConnector.getPort());
    final Context root = new Context(jettyServer, "/", Context.SESSIONS);
    root.addServlet(new ServletHolder(new SimpleServlet()), "/simple");
    jettyServer.start();
    final ExecutorApiClient tlsDisabledClient = new ExecutorApiClient(new Props());
    final String postResponse = tlsDisabledClient
        .doPost(new URI(SimpleServlet.TLS_DISABLED_URI), DispatchMethod.CONTAINERIZED,
            Optional.of(-1),null);
    Assert.assertEquals(SimpleServlet.POST_RESPONSE_STRING, postResponse);
    jettyServer.stop();
  }

  /**
   * This test tries to create the jetty-server and verify that it is reachable on the attached port
   * via Https.
   */
  @Test
  public void testSslEnabledJettyServer() throws Exception {
    ExecJettyServerModule execJettyServerModule = new ExecJettyServerModule();

    Props props = new Props();
    props.put("jetty.use.ssl", "true");
    props.put("executor.ssl.port", JETTY_TLS_PORT);
    props.put("jetty.keystore", KEYSTORE_PATH);
    props.put("jetty.password", DEFAULT_PASSWORD);
    props.put("jetty.keypassword", DEFAULT_PASSWORD);
    props.put("jetty.truststore", TRUSTSTORE_PATH);
    props.put("jetty.trustpassword", DEFAULT_PASSWORD);
    Server jettyServer = execJettyServerModule.createJettyServer(props);
    Assert.assertEquals(1, jettyServer.getConnectors().length);
    Assert.assertTrue(jettyServer.getConnectors()[0] instanceof SslSocketConnector);
    SslSocketConnector sslSocketConnector = (SslSocketConnector) jettyServer.getConnectors()[0];
    Assert.assertEquals(JETTY_TLS_PORT, sslSocketConnector.getPort());
    final Context root = new Context(jettyServer, "/", Context.SESSIONS);
    root.addServlet(new ServletHolder(new SimpleServlet()), "/simple");
    jettyServer.start();

    Props clientProps = new Props();
    clientProps.put(EXECUTOR_CLIENT_TLS_ENABLED, "true");
    clientProps
        .put(EXECUTOR_CLIENT_TRUSTSTORE_PATH, TRUSTSTORE_PATH);
    clientProps.put(EXECUTOR_CLIENT_TRUSTSTORE_PASSWORD, "changeit");

    final ExecutorApiClient tlsEnabledClient = new ExecutorApiClient(clientProps);
    final String postResponse = tlsEnabledClient
        .doPost(new URI(SimpleServlet.TLS_ENABLED_URI), DispatchMethod.CONTAINERIZED,
            Optional.of(-1),null);
    Assert.assertEquals(SimpleServlet.POST_RESPONSE_STRING, postResponse);
    jettyServer.stop();
  }
}

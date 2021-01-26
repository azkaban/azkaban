/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.webapp;

import static azkaban.Constants.ConfigurationKeys.JETTY_PORT;
import static azkaban.Constants.ConfigurationKeys.JETTY_SSL_PORT;
import static azkaban.Constants.ConfigurationKeys.JETTY_USE_SSL;
import static azkaban.Constants.DEFAULT_JETTY_MAX_THREAD_COUNT;
import static azkaban.Constants.DEFAULT_PORT_NUMBER;
import static azkaban.Constants.DEFAULT_SSL_PORT_NUMBER;
import static java.util.Objects.requireNonNull;

import azkaban.utils.Props;
import com.google.inject.Provider;
import java.util.List;
import javax.inject.Inject;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;


public class WebServerProvider implements Provider<Server> {

  private static final Logger logger = Logger.getLogger(WebServerProvider.class);
  private static final int DEFAULT_HEADER_BUFFER_SIZE = 10 * 1024 * 1024;

  @Inject
  private Props props;

  @Override
  public Server get() {
    requireNonNull(this.props);

    final boolean useSsl = this.props.getBoolean(JETTY_USE_SSL, true);
    final int port;
    final Server server = new Server();
    if (useSsl) {
      final int sslPortNumber = this.props
          .getInt(JETTY_SSL_PORT, DEFAULT_SSL_PORT_NUMBER);
      port = sslPortNumber;
      server.addConnector(getSslSocketConnector(sslPortNumber));
    } else {
      port = this.props.getInt(JETTY_PORT, DEFAULT_PORT_NUMBER);
      server.addConnector(getSocketConnector(port));
    }

    // Configure the ThreadPool
    final int maxThreads = this.props
        .getInt("jetty.maxThreads", DEFAULT_JETTY_MAX_THREAD_COUNT);
    final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads);
    server.setThreadPool(threadPool);

    // setting stats configuration for connectors
    setStatsOnConnectors(server);

    logger.info(String.format(
        "Starting %sserver on port: %d # Max threads: %d", useSsl ? "SSL " : "", port, maxThreads));
    return server;
  }

  private SocketConnector getSocketConnector(final int port) {
    final SocketConnector connector = new SocketConnector();
    connector.setPort(port);
    connector.setRequestBufferSize(DEFAULT_HEADER_BUFFER_SIZE);
    connector.setResponseBufferSize(DEFAULT_HEADER_BUFFER_SIZE);
    return connector;
  }

  private SslSocketConnector getSslSocketConnector(final int sslPortNumber) {
    final SslSocketConnector secureConnector = new SslSocketConnector();
    secureConnector.setPort(sslPortNumber);
    secureConnector.setKeystore(this.props.getString("jetty.keystore"));
    secureConnector.setPassword(this.props.getString("jetty.password"));
    secureConnector.setKeyPassword(this.props.getString("jetty.keypassword"));
    secureConnector.setTruststore(this.props.getString("jetty.truststore"));
    secureConnector.setTrustPassword(this.props.getString("jetty.trustpassword"));
    secureConnector.setRequestBufferSize(DEFAULT_HEADER_BUFFER_SIZE);
    secureConnector.setResponseBufferSize(DEFAULT_HEADER_BUFFER_SIZE);

    // set up vulnerable cipher suites to exclude
    final List<String> cipherSuitesToExclude = this.props
        .getStringList("jetty.excludeCipherSuites");
    logger.info("Excluded Cipher Suites: " + cipherSuitesToExclude);
    if (cipherSuitesToExclude != null && !cipherSuitesToExclude.isEmpty()) {
      secureConnector.setExcludeCipherSuites(cipherSuitesToExclude.toArray(new String[0]));
    }
    return secureConnector;
  }

  private void setStatsOnConnectors(final Server server) {
    final boolean isStatsOn = this.props.getBoolean("jetty.connector.stats", true);
    logger.info("Setting up connector with stats on: " + isStatsOn);
    for (final Connector connector : server.getConnectors()) {
      connector.setStatsOn(isStatsOn);
    }
  }
}

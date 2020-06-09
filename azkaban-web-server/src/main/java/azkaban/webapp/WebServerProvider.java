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

import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.utils.Props;
import com.google.inject.Provider;
import java.util.List;
import javax.inject.Inject;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.thread.QueuedThreadPool;


public class WebServerProvider implements Provider<Server> {

  private static final Logger logger = Logger.getLogger(WebServerProvider.class);
  private static final int MAX_HEADER_BUFFER_SIZE = 10 * 1024 * 1024;

  @Inject
  private Props props;

  @Override
  public Server get() {
    requireNonNull(this.props);

    final boolean useSsl = this.props.getBoolean("jetty.use.ssl", true);
    final int port;
    final Server server = new Server();
    if (useSsl) {
      final int sslPortNumber = this.props
          .getInt("jetty.ssl.port", Constants.DEFAULT_SSL_PORT_NUMBER);
      port = sslPortNumber;
      server.addConnector(getSslSocketConnector(sslPortNumber));
    } else {
      port = this.props.getInt("jetty.port", Constants.DEFAULT_PORT_NUMBER);
      server.addConnector(getSocketConnector(port));
    }

    // Configure the ThreadPool
    final int maxThreads = this.props
        .getInt("jetty.maxThreads", Constants.DEFAULT_JETTY_MAX_THREAD_COUNT);
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
    connector.setHeaderBufferSize(MAX_HEADER_BUFFER_SIZE);
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
    secureConnector.setHeaderBufferSize(MAX_HEADER_BUFFER_SIZE);

    // set up vulnerable cipher suites to exclude
    final List<String> cipherSuitesToExclude = this.props
        .getStringList("jetty.excludeCipherSuites");
    logger.info("Excluded Cipher Suites: " + String.valueOf(cipherSuitesToExclude));
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

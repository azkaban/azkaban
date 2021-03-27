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

import azkaban.server.JettyServerUtils;
import azkaban.utils.Props;
import com.google.inject.Provider;
import javax.inject.Inject;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.thread.QueuedThreadPool;


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
      port = this.props
          .getInt(JETTY_SSL_PORT, DEFAULT_SSL_PORT_NUMBER);
      server.addConnector(JettyServerUtils.getSslSocketConnector(port, this.props));
      logger.info("Added SslSocketConnector as Ssl is enabled");
    } else {
      port = this.props.getInt(JETTY_PORT, DEFAULT_PORT_NUMBER);
      server.addConnector(JettyServerUtils.getSocketConnector(port));
      logger.info("Added SocketConnector as Ssl is disabled");
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

  private void setStatsOnConnectors(final Server server) {
    final boolean isStatsOn = this.props.getBoolean("jetty.connector.stats", true);
    logger.info("Setting up connector with stats on: " + isStatsOn);
    for (final Connector connector : server.getConnectors()) {
      connector.setStatsOn(isStatsOn);
    }
  }
}

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

import azkaban.Constants;
import azkaban.utils.Props;
import com.google.inject.Inject;
import com.google.inject.Provider;
import java.util.List;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.security.SslSocketConnector;

import static java.util.Objects.*;


public class WebServerProvider implements Provider<Server> {
  private static final Logger logger = Logger.getLogger(WebServerProvider.class);
  private static final int MAX_HEADER_BUFFER_SIZE = 10 * 1024 * 1024;

  @Inject
  private Props props;

  @Override
  public Server get() {
    requireNonNull(props);

    final int maxThreads = props.getInt("jetty.maxThreads", Constants.DEFAULT_JETTY_MAX_THREAD_COUNT);
    boolean isStatsOn = props.getBoolean("jetty.connector.stats", true);
    logger.info("Setting up connector with stats on: " + isStatsOn);

    boolean ssl;
    int port;
    final Server server = new Server();
    if (props.getBoolean("jetty.use.ssl", true)) {
      int sslPortNumber = props.getInt("jetty.ssl.port", Constants.DEFAULT_SSL_PORT_NUMBER);
      port = sslPortNumber;
      ssl = true;
      logger.info("Setting up Jetty Https Server with port:" + sslPortNumber
          + " and numThreads:" + maxThreads);

      SslSocketConnector secureConnector = new SslSocketConnector();
      secureConnector.setPort(sslPortNumber);
      secureConnector.setKeystore(props.getString("jetty.keystore"));
      secureConnector.setPassword(props.getString("jetty.password"));
      secureConnector.setKeyPassword(props.getString("jetty.keypassword"));
      secureConnector.setTruststore(props.getString("jetty.truststore"));
      secureConnector.setTrustPassword(props.getString("jetty.trustpassword"));
      secureConnector.setHeaderBufferSize(MAX_HEADER_BUFFER_SIZE);

      // set up vulnerable cipher suites to exclude
      List<String> cipherSuitesToExclude = props.getStringList("jetty.excludeCipherSuites");
      logger.info("Excluded Cipher Suites: " + String.valueOf(cipherSuitesToExclude));
      if (cipherSuitesToExclude != null && !cipherSuitesToExclude.isEmpty()) {
        secureConnector.setExcludeCipherSuites(cipherSuitesToExclude.toArray(new String[0]));
      }

      server.addConnector(secureConnector);
    } else {
      ssl = false;
      port = props.getInt("jetty.port", Constants.DEFAULT_PORT_NUMBER);
      SocketConnector connector = new SocketConnector();
      connector.setPort(port);
      connector.setHeaderBufferSize(MAX_HEADER_BUFFER_SIZE);
      server.addConnector(connector);
    }

    // setting stats configuration for connectors
    for (Connector connector : server.getConnectors()) {
      connector.setStatsOn(isStatsOn);
    }

    logger.info(String.format("Starting %sserver on port: %d", ssl ? "SSL " : "", port));
    return server;
  }
}

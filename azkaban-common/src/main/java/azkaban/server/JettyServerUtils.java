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
 */
package azkaban.server;

import azkaban.utils.Props;
import java.util.List;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for adding creating connectors, configurations, etc. to the Jetty Server.
 */
public class JettyServerUtils {

  private static final int DEFAULT_HEADER_BUFFER_SIZE = 10 * 1024 * 1024;
  private static final Logger logger = LoggerFactory.getLogger(JettyServerUtils.class);
  private static final String JETTY_KEYSTORE = "jetty.keystore";
  private static final String JETTY_PASSWORD = "jetty.password";
  private static final String JETTY_KEYPASSWORD = "jetty.keypassword";
  private static final String JETTY_TRUSTSTORE = "jetty.truststore";
  private static final String JETTY_TRUSTPASSWORD = "jetty.trustpassword";

  private JettyServerUtils() {
    // Not to be instantiated
  }

  /**
   * @param sslPortNumber Port number to bind for the socket
   * @param props         Azkaban properties containing configurations for Jetty Server
   * @return SSL enabled SocketConnector {@link SslSocketConnector}, for https connection to the
   * jetty server
   */
  public static SslSocketConnector getSslSocketConnector(final int sslPortNumber,
      final Props props) {
    final SslSocketConnector secureConnector = new SslSocketConnector();
    secureConnector.setPort(sslPortNumber);
    secureConnector.setKeystore(props.getString(JETTY_KEYSTORE));
    secureConnector.setPassword(props.getString(JETTY_PASSWORD));
    secureConnector.setKeyPassword(props.getString(JETTY_KEYPASSWORD));
    secureConnector.setTruststore(props.getString(JETTY_TRUSTSTORE));
    secureConnector.setTrustPassword(props.getString(JETTY_TRUSTPASSWORD));
    secureConnector.setHeaderBufferSize(DEFAULT_HEADER_BUFFER_SIZE);

    // set up vulnerable cipher suites to exclude
    final List<String> cipherSuitesToExclude = props
        .getStringList("jetty.excludeCipherSuites");
    logger.info("Excluded Cipher Suites: " + String.valueOf(cipherSuitesToExclude));
    if (cipherSuitesToExclude != null && !cipherSuitesToExclude.isEmpty()) {
      secureConnector.setExcludeCipherSuites(cipherSuitesToExclude.toArray(new String[0]));
    }
    return secureConnector;
  }

  /**
   * @param port Port number to bind for the socket
   * @return A plain {@link SocketConnector} for http connection to the jetty server
   */
  public static SocketConnector getSocketConnector(final int port) {
    final SocketConnector connector = new SocketConnector();
    connector.setPort(port);
    connector.setHeaderBufferSize(DEFAULT_HEADER_BUFFER_SIZE);
    return connector;
  }
}

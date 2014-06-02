/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.jmx;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;

public class JmxJettyServer implements JmxJettyServerMBean {
  private Server server;
  private Connector connector;

  public JmxJettyServer(Server server) {
    this.server = server;
    this.connector = server.getConnectors()[0];
  }

  @Override
  public boolean isRunning() {
    return this.server.isRunning();
  }

  @Override
  public boolean isFailed() {
    return this.server.isFailed();
  }

  @Override
  public boolean isStopped() {
    return this.server.isStopped();
  }

  @Override
  public int getNumThreads() {
    return this.server.getThreadPool().getThreads();
  }

  @Override
  public int getNumIdleThreads() {
    return this.server.getThreadPool().getIdleThreads();
  }

  @Override
  public String getHost() {
    return connector.getHost();
  }

  @Override
  public int getPort() {
    return connector.getPort();
  }

  @Override
  public int getConfidentialPort() {
    return connector.getConfidentialPort();
  }

  @Override
  public int getConnections() {
    return connector.getConnections();
  }

  @Override
  public int getConnectionsOpen() {
    return connector.getConnectionsOpen();
  }

  @Override
  public int getConnectionsOpenMax() {
    return connector.getConnectionsOpenMax();
  }

  @Override
  public int getConnectionsOpenMin() {
    return connector.getConnectionsOpenMin();
  }

  @Override
  public long getConnectionsDurationAve() {
    return connector.getConnectionsDurationAve();
  }

  @Override
  public long getConnectionsDurationMax() {
    return connector.getConnectionsDurationMax();
  }

  @Override
  public long getConnectionsDurationMin() {
    return connector.getConnectionsDurationMin();
  }

  @Override
  public long getConnectionsDurationTotal() {
    return connector.getConnectionsDurationTotal();
  }

  @Override
  public long getConnectionsRequestAve() {
    return connector.getConnectionsRequestsAve();
  }

  @Override
  public long getConnectionsRequestMax() {
    return connector.getConnectionsRequestsMax();
  }

  @Override
  public long getConnectionsRequestMin() {
    return connector.getConnectionsRequestsMin();
  }

  @Override
  public void turnStatsOn() {
    connector.setStatsOn(true);
  }

  @Override
  public void turnStatsOff() {
    connector.setStatsOn(false);
  }

  @Override
  public void resetStats() {
    connector.statsReset();
  }

  @Override
  public boolean isStatsOn() {
    return connector.getStatsOn();
  }
}

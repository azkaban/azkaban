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

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;

public class JmxJettyServer implements JmxJettyServerMBean {

  private final Server server;
  private final Connector connector;

  public JmxJettyServer(final Server server) {
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
    return this.connector.getHost();
  }

  @Override
  public int getPort() {
    return this.connector.getPort();
  }

  @Override
  public int getConfidentialPort() {
    return this.connector.getConfidentialPort();
  }

  @Override
  public int getConnections() {
    return this.connector.getConnections();
  }

  @Override
  public int getConnectionsOpen() {
    return this.connector.getConnectionsOpen();
  }

  @Override
  public int getConnectionsOpenMax() {
    return this.connector.getConnectionsOpenMax();
  }

  @Override
  public int getConnectionsOpenMin() {
    // @TODO fix jetty upgrade
    // return this.connector.getConnectionsOpenMin();
    return this.connector.getConnectionsOpen();
  }

  @Override
  public long getConnectionsDurationAve() {
    return (long) this.connector.getConnectionsDurationMean();
  }

  @Override
  public long getConnectionsDurationMax() {
    return this.connector.getConnectionsDurationMax();
  }

  @Override
  public long getConnectionsDurationMin() {
    // @TODO fix jetty upgrade
    return (long) this.connector.getConnectionsRequestsMean();
  }

  @Override
  public long getConnectionsDurationTotal() {
    return this.connector.getConnectionsDurationTotal();
  }

  @Override
  public long getConnectionsRequestAve() {
    return (long) this.connector.getConnectionsRequestsMean();
  }

  @Override
  public long getConnectionsRequestMax() {
    return this.connector.getConnectionsRequestsMax();
  }

  @Override
  public long getConnectionsRequestMin() {
    // @TODO fix jetty upgrade getConnectionsRequestsMin
    return (long) this.connector.getConnectionsRequestsMean();
  }

  @Override
  public void turnStatsOn() {
    this.connector.setStatsOn(true);
  }

  @Override
  public void turnStatsOff() {
    this.connector.setStatsOn(false);
  }

  @Override
  public void resetStats() {
    this.connector.statsReset();
  }

  @Override
  public boolean isStatsOn() {
    return this.connector.getStatsOn();
  }
}

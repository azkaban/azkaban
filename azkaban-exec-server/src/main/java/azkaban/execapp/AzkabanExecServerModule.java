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

package azkaban.execapp;

import azkaban.executor.ExecutorLoader;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import javax.inject.Named;
import javax.inject.Singleton;


/**
 * This Guice module is currently a one place container for all bindings in the current module. This is intended to
 * help during the migration process to Guice. Once this class starts growing we can move towards more modular
 * structuring of Guice components.
 */
public class AzkabanExecServerModule extends AbstractModule {

  private static final int DEFAULT_THREAD_NUMBER = 50;
  private static final int DEFAULT_HEADER_BUFFER_SIZE = 4096;
  private static final int MAX_FORM_CONTENT_SIZE = 10 * 1024 * 1024;

  private static final Logger logger = Logger.getLogger(AzkabanExecServerModule.class);

  @Override
  protected void configure() {
    bind(ExecutorLoader.class).to(JdbcExecutorLoader.class).in(Scopes.SINGLETON);
    bind(AzkabanExecutorServer.class).in(Scopes.SINGLETON);
    bind(TriggerManager.class).in(Scopes.SINGLETON);
    bind(FlowRunnerManager.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Named("ExecServer")
  @Singleton
  private Server createJettyServer(Props props) {
    int maxThreads = props.getInt("executor.maxThreads", DEFAULT_THREAD_NUMBER);

    /*
     * Default to a port number 0 (zero)
     * The Jetty server automatically finds an unused port when the port number is set to zero
     * TODO: This is using a highly outdated version of jetty [year 2010]. needs to be updated.
     */
    Server server = new Server(props.getInt("executor.port", 0));
    QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
    server.setThreadPool(httpThreadPool);

    boolean isStatsOn = props.getBoolean("executor.connector.stats", true);
    logger.info("Setting up connector with stats on: " + isStatsOn);

    for (Connector connector : server.getConnectors()) {
      connector.setStatsOn(isStatsOn);
      logger.info(String.format(
              "Jetty connector name: %s, default header buffer size: %d",
              connector.getName(), connector.getHeaderBufferSize()));
      connector.setHeaderBufferSize(props.getInt("jetty.headerBufferSize",
              DEFAULT_HEADER_BUFFER_SIZE));
      logger.info(String.format(
              "Jetty connector name: %s, (if) new header buffer size: %d",
              connector.getName(), connector.getHeaderBufferSize()));
    }

    return server;
  }

  @Provides
  @Named("root")
  @Singleton
  private Context createRootContext(Server server) {
    Context root = new Context(server, "/", Context.SESSIONS);
    root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);

    root.addServlet(new ServletHolder(new ExecutorServlet()), "/executor");
    root.addServlet(new ServletHolder(new JMXHttpServlet()), "/jmx");
    root.addServlet(new ServletHolder(new StatsServlet()), "/stats");
    root.addServlet(new ServletHolder(new ServerStatisticsServlet()), "/serverStatistics");
    return root;
  }
}

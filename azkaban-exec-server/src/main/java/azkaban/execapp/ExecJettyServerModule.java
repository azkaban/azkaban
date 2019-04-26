package azkaban.execapp;

import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import javax.inject.Named;
import javax.inject.Singleton;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecJettyServerModule extends AbstractModule {

  public static final String EXEC_JETTY_SERVER = "ExecServer";
  public static final String EXEC_ROOT_CONTEXT = "root";

  private static final int DEFAULT_THREAD_NUMBER = 50;
  private static final int DEFAULT_HEADER_BUFFER_SIZE = 4096;
  private static final int MAX_FORM_CONTENT_SIZE = 10 * 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(ExecJettyServerModule.class);

  @Override
  protected void configure() {
  }

  @Provides
  @Named(EXEC_JETTY_SERVER)
  @Singleton
  private Server createJettyServer(final Props props) {
    final int maxThreads = props.getInt("executor.maxThreads", DEFAULT_THREAD_NUMBER);

    /*
     * Default to a port number 0 (zero)
     * The Jetty server automatically finds an unused port when the port number is set to zero
     * TODO: This is using a highly outdated version of jetty [year 2010]. needs to be updated.
     */
    final Server server = new Server(props.getInt(ConfigurationKeys.EXECUTOR_PORT, 0));
    final QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
    server.setThreadPool(httpThreadPool);

    final boolean isStatsOn = props.getBoolean("executor.connector.stats", true);
    LOG.info("Setting up connector with stats on: " + isStatsOn);

    for (final Connector connector : server.getConnectors()) {
      connector.setStatsOn(isStatsOn);
      LOG.info(String.format(
          "Jetty connector name: %s, default header buffer size: %d",
          connector.getName(), connector.getHeaderBufferSize()));
      connector.setHeaderBufferSize(props.getInt("jetty.headerBufferSize",
          DEFAULT_HEADER_BUFFER_SIZE));
      LOG.info(String.format(
          "Jetty connector name: %s, (if) new header buffer size: %d",
          connector.getName(), connector.getHeaderBufferSize()));
    }

    return server;
  }

  @Provides
  @Named(EXEC_ROOT_CONTEXT)
  @Singleton
  private Context createRootContext(@Named(EXEC_JETTY_SERVER) final Server server) {
    final Context root = new Context(server, "/", Context.SESSIONS);
    root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);

    root.addServlet(new ServletHolder(new ExecutorServlet()), "/executor");
    root.addServlet(new ServletHolder(new JMXHttpServlet()), "/jmx");
    root.addServlet(new ServletHolder(new StatsServlet()), "/stats");
    root.addServlet(new ServletHolder(new ServerStatisticsServlet()), "/serverStatistics");
    return root;
  }
}

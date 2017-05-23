package azkaban.execapp;

import azkaban.utils.Props;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.thread.QueuedThreadPool;

public class ExecServerProvider implements Provider<Server> {

  private static final int DEFAULT_THREAD_NUMBER = 50;
  private static final int DEFAULT_HEADER_BUFFER_SIZE = 4096;

  private static final Logger logger = Logger.getLogger(ExecServerProvider.class);

  @Inject
  private Props props;

  @Override
  public Server get() {
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

}

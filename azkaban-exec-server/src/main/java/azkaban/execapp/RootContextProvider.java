package azkaban.execapp;

import com.google.inject.Provider;
import javax.inject.Inject;
import javax.inject.Named;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class RootContextProvider implements Provider<Context> {

  private static final int MAX_FORM_CONTENT_SIZE = 10 * 1024 * 1024;

  @Inject
  @Named("ExecServer")
  private Server server;

  @Override
  public Context get() {
    Context root = new Context(server, "/", Context.SESSIONS);
    root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);

    root.addServlet(new ServletHolder(new ExecutorServlet()), "/executor");
    root.addServlet(new ServletHolder(new JMXHttpServlet()), "/jmx");
    root.addServlet(new ServletHolder(new StatsServlet()), "/stats");
    root.addServlet(new ServletHolder(new ServerStatisticsServlet()), "/serverStatistics");
    return root;
  }

}

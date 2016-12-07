/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.execapp;

import com.google.common.base.Throwables;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.codahale.metrics.MetricRegistry;

import azkaban.constants.ServerInternals;
import azkaban.constants.ServerProperties;

import azkaban.execapp.event.JobCallbackManager;
import azkaban.execapp.jmx.JmxFlowRunnerManager;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.execapp.metric.NumFailedFlowMetric;
import azkaban.execapp.metric.NumFailedJobMetric;
import azkaban.execapp.metric.NumQueuedFlowMetric;
import azkaban.execapp.metric.NumRunningFlowMetric;
import azkaban.execapp.metric.NumRunningJobMetric;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.jmx.JmxJettyServer;
import azkaban.metric.IMetricEmitter;
import azkaban.metric.MetricException;
import azkaban.metric.MetricReportManager;
import azkaban.metric.inmemoryemitter.InMemoryMetricEmitter;
import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectLoader;
import azkaban.server.AzkabanServer;
import azkaban.utils.Props;
import azkaban.utils.StdOutErrRedirect;
import azkaban.utils.SystemMemoryInfo;
import azkaban.utils.Utils;
import azkaban.metrics.MetricsManager;

import static azkaban.constants.ServerInternals.AZKABAN_EXECUTOR_PORT_FILENAME;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AzkabanExecutorServer {
  private static final String CUSTOM_JMX_ATTRIBUTE_PROCESSOR_PROPERTY = "jmx.attribute.processor.class";
  private static final Logger logger = Logger.getLogger(AzkabanExecutorServer.class);
  private static final int MAX_FORM_CONTENT_SIZE = 10 * 1024 * 1024;

  public static final String JOBTYPE_PLUGIN_DIR = "azkaban.jobtype.plugin.dir";
  public static final String METRIC_INTERVAL = "executor.metric.milisecinterval.";
  public static final int DEFAULT_HEADER_BUFFER_SIZE = 4096;

  private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";
  private static final int DEFAULT_THREAD_NUMBER = 50;

  private static AzkabanExecutorServer app;

  private final ExecutorLoader executionLoader;
  private final ProjectLoader projectLoader;
  private final FlowRunnerManager runnerManager;
  private final Props props;
  private final Server server;

  private final ArrayList<ObjectName> registeredMBeans = new ArrayList<ObjectName>();
  private MBeanServer mbeanServer;

  /**
   * Constructor
   *
   * @throws Exception
   */
  public AzkabanExecutorServer(Props props) throws Exception {
    this.props = props;
    server = createJettyServer(props);

    executionLoader = new JdbcExecutorLoader(props);
    projectLoader = new JdbcProjectLoader(props);
    runnerManager = new FlowRunnerManager(props, executionLoader, projectLoader, getClass().getClassLoader());

    JmxJobMBeanManager.getInstance().initialize(props);

    // make sure this happens before
    configureJobCallback(props);

    configureMBeanServer();
    configureMetricReports();

    SystemMemoryInfo.init(props.getInt("executor.memCheck.interval", 30));

    loadCustomJMXAttributeProcessor(props);

    try {
      server.start();
    } catch (Exception e) {
      logger.error(e);
      Utils.croak(e.getMessage(), 1);
    }

    insertExecutorEntryIntoDB();
    dumpPortToFile();

    logger.info("Started Executor Server on " + getExecutorHostPort());

    if (props.getBoolean(ServerProperties.IS_METRICS_ENABLED, false)) {
      startExecMetrics();
    }
  }

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

    Context root = new Context(server, "/", Context.SESSIONS);
    root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);

    root.addServlet(new ServletHolder(new ExecutorServlet()), "/executor");
    root.addServlet(new ServletHolder(new JMXHttpServlet()), "/jmx");
    root.addServlet(new ServletHolder(new StatsServlet()), "/stats");
    root.addServlet(new ServletHolder(new ServerStatisticsServlet()), "/serverStatistics");

    root.setAttribute(ServerInternals.AZKABAN_SERVLET_CONTEXT_KEY, this);
    return server;
  }

  private void startExecMetrics() throws Exception {
    MetricRegistry metrics = MetricsManager.INSTANCE.getRegistry();

    logger.info("starting reporting Executor Metrics");
    MetricsExecRegister execWorker =
        new MetricsExecRegister.MetricsExecRegisterBuilder("EXEC").addFlowRunnerManager(getFlowRunnerManager()).build();
    execWorker.addExecutorManagerMetrics(metrics);

    MetricsManager.INSTANCE.startReporting("AZ-EXEC", props);
  }

  private void insertExecutorEntryIntoDB() {
    try {
      final String host = requireNonNull(getHost());
      final int port = getPort();
      checkState(port != -1);
      final Executor executor = executionLoader.fetchExecutor(host, port);
      if (executor == null) {
        executionLoader.addExecutor(host, port);
      }
      // If executor already exists, ignore it
    } catch (ExecutorManagerException e) {
      logger.error("Error inserting executor entry into DB", e);
      Throwables.propagate(e);
    }
  }

  private void dumpPortToFile() {
    // By default this should write to the working directory
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(AZKABAN_EXECUTOR_PORT_FILENAME))) {
      writer.write(String.valueOf(getPort()));
      writer.write("\n");
    } catch (IOException e) {
      logger.error(e);
      Throwables.propagate(e);
    }
  }

  private void configureJobCallback(Props props) {
    boolean jobCallbackEnabled =
        props.getBoolean("azkaban.executor.jobcallback.enabled", true);

    logger.info("Job callback enabled? " + jobCallbackEnabled);

    if (jobCallbackEnabled) {
      JobCallbackManager.initialize(props);
    }
  }

  /**
   * Configure Metric Reporting as per azkaban.properties settings
   *
   * @throws MetricException
   */
  private void configureMetricReports() throws MetricException {
    Props props = getAzkabanProps();
    if (props != null && props.getBoolean("executor.metric.reports", false)) {
      logger.info("Starting to configure Metric Reports");
      MetricReportManager metricManager = MetricReportManager.getInstance();
      IMetricEmitter metricEmitter = new InMemoryMetricEmitter(props);
      metricManager.addMetricEmitter(metricEmitter);

      logger.info("Adding number of failed flow metric");
      metricManager.addMetric(new NumFailedFlowMetric(metricManager, props
          .getInt(METRIC_INTERVAL
              + NumFailedFlowMetric.NUM_FAILED_FLOW_METRIC_NAME,
              props.getInt(METRIC_INTERVAL + "default"))));

      logger.info("Adding number of failed jobs metric");
      metricManager.addMetric(new NumFailedJobMetric(metricManager, props
          .getInt(METRIC_INTERVAL
              + NumFailedJobMetric.NUM_FAILED_JOB_METRIC_NAME,
              props.getInt(METRIC_INTERVAL + "default"))));

      logger.info("Adding number of running Jobs metric");
      metricManager.addMetric(new NumRunningJobMetric(metricManager, props
          .getInt(METRIC_INTERVAL
              + NumRunningJobMetric.NUM_RUNNING_JOB_METRIC_NAME,
              props.getInt(METRIC_INTERVAL + "default"))));

      logger.info("Adding number of running flows metric");
      metricManager.addMetric(new NumRunningFlowMetric(runnerManager,
          metricManager, props.getInt(METRIC_INTERVAL
              + NumRunningFlowMetric.NUM_RUNNING_FLOW_METRIC_NAME,
              props.getInt(METRIC_INTERVAL + "default"))));

      logger.info("Adding number of queued flows metric");
      metricManager.addMetric(new NumQueuedFlowMetric(runnerManager,
          metricManager, props.getInt(METRIC_INTERVAL
              + NumQueuedFlowMetric.NUM_QUEUED_FLOW_METRIC_NAME,
              props.getInt(METRIC_INTERVAL + "default"))));

      logger.info("Completed configuring Metric Reports");
    }

  }

  /**
   * Load a custom class, which is provided by a configuration
   * CUSTOM_JMX_ATTRIBUTE_PROCESSOR_PROPERTY.
   *
   * This method will try to instantiate an instance of this custom class and
   * with given properties as the argument in the constructor.
   *
   * Basically the custom class must have a constructor that takes an argument
   * with type Properties.
   *
   * @param props
   */
  private void loadCustomJMXAttributeProcessor(Props props) {
    String jmxAttributeEmitter =
        props.get(CUSTOM_JMX_ATTRIBUTE_PROCESSOR_PROPERTY);
    if (jmxAttributeEmitter != null) {
      try {
        logger.info("jmxAttributeEmitter: " + jmxAttributeEmitter);
        Constructor<Props>[] constructors =
            (Constructor<Props>[]) Class.forName(jmxAttributeEmitter)
                .getConstructors();

        constructors[0].newInstance(props.toProperties());
      } catch (Exception e) {
        logger.error("Encountered error while loading and instantiating "
            + jmxAttributeEmitter, e);
        throw new IllegalStateException(
            "Encountered error while loading and instantiating "
                + jmxAttributeEmitter, e);
      }
    } else {
      logger.info("No value for property: "
          + CUSTOM_JMX_ATTRIBUTE_PROCESSOR_PROPERTY + " was found");
    }
  }


  public ProjectLoader getProjectLoader() {
    return projectLoader;
  }

  public ExecutorLoader getExecutorLoader() {
    return executionLoader;
  }

  /**
   * Returns the global azkaban properties
   *
   * @return
   */
  public Props getAzkabanProps() {
    return props;
  }

  /**
   * Returns the currently executing executor server, if one exists.
   *
   * @return
   */
  public static AzkabanExecutorServer getApp() {
    return app;
  }

  /**
   * Azkaban using Jetty
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws Exception {
    // Redirect all std out and err messages into log4j
    StdOutErrRedirect.redirectOutAndErrToLog();

    logger.info("Starting Jetty Azkaban Executor...");
    Props azkabanSettings = AzkabanServer.loadProps(args);

    if (azkabanSettings == null) {
      logger.error("Azkaban Properties not loaded.");
      logger.error("Exiting Azkaban Executor Server...");
      return;
    }

    // Setup time zone
    if (azkabanSettings.containsKey(DEFAULT_TIMEZONE_ID)) {
      String timezone = azkabanSettings.getString(DEFAULT_TIMEZONE_ID);
      System.setProperty("user.timezone", timezone);
      TimeZone.setDefault(TimeZone.getTimeZone(timezone));
      DateTimeZone.setDefault(DateTimeZone.forID(timezone));

      logger.info("Setting timezone to " + timezone);
    }

    app = new AzkabanExecutorServer(azkabanSettings);

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          logTopMemoryConsumers();
        } catch (Exception e) {
          logger.info(("Exception when logging top memory consumers"), e);
        }

        logger.info("Shutting down...");
        try {
          app.shutdownNow();
        } catch (Exception e) {
          logger.error("Error while shutting down http server.", e);
        }
      }

      public void logTopMemoryConsumers() throws Exception, IOException {
        if (new File("/bin/bash").exists() && new File("/bin/ps").exists()
            && new File("/usr/bin/head").exists()) {
          logger.info("logging top memeory consumer");

          java.lang.ProcessBuilder processBuilder =
              new java.lang.ProcessBuilder("/bin/bash", "-c",
                  "/bin/ps aux --sort -rss | /usr/bin/head");
          Process p = processBuilder.start();
          p.waitFor();

          InputStream is = p.getInputStream();
          java.io.BufferedReader reader =
              new java.io.BufferedReader(new InputStreamReader(is));
          String line = null;
          while ((line = reader.readLine()) != null) {
            logger.info(line);
          }
          is.close();
        }
      }
    });
  }

  /**
   * Loads the Azkaban property file from the AZKABAN_HOME conf directory
   *
   * @return
   */
  /* package */static Props loadConfigurationFromAzkabanHome() {
    String azkabanHome = System.getenv("AZKABAN_HOME");

    if (azkabanHome == null) {
      logger.error("AZKABAN_HOME not set. Will try default.");
      return null;
    }

    if (!new File(azkabanHome).isDirectory()
        || !new File(azkabanHome).canRead()) {
      logger.error(azkabanHome + " is not a readable directory.");
      return null;
    }

    File confPath = new File(azkabanHome, ServerInternals.DEFAULT_CONF_PATH);
    if (!confPath.exists() || !confPath.isDirectory() || !confPath.canRead()) {
      logger
          .error(azkabanHome + " does not contain a readable conf directory.");
      return null;
    }

    return loadAzkabanConfigurationFromDirectory(confPath);
  }

  public FlowRunnerManager getFlowRunnerManager() {
    return runnerManager;
  }

  /**
   * Loads the Azkaban conf file int a Props object
   *
   * @return
   */
  private static Props loadAzkabanConfigurationFromDirectory(File dir) {
    File azkabanPrivatePropsFile =
        new File(dir, ServerInternals.AZKABAN_PRIVATE_PROPERTIES_FILE);
    File azkabanPropsFile = new File(dir, ServerInternals.AZKABAN_PROPERTIES_FILE);

    Props props = null;
    try {
      // This is purely optional
      if (azkabanPrivatePropsFile.exists() && azkabanPrivatePropsFile.isFile()) {
        logger.info("Loading azkaban private properties file");
        props = new Props(null, azkabanPrivatePropsFile);
      }

      if (azkabanPropsFile.exists() && azkabanPropsFile.isFile()) {
        logger.info("Loading azkaban properties file");
        props = new Props(props, azkabanPropsFile);
      }
    } catch (FileNotFoundException e) {
      logger.error("File not found. Could not load azkaban config file", e);
    } catch (IOException e) {
      logger.error(
          "File found, but error reading. Could not load azkaban config file",
          e);
    }

    return props;
  }

  private void configureMBeanServer() {
    logger.info("Registering MBeans...");
    mbeanServer = ManagementFactory.getPlatformMBeanServer();

    registerMbean("executorJetty", new JmxJettyServer(server));
    registerMbean("flowRunnerManager", new JmxFlowRunnerManager(runnerManager));
    registerMbean("jobJMXMBean", JmxJobMBeanManager.getInstance());

    if (JobCallbackManager.isInitialized()) {
      JobCallbackManager jobCallbackMgr = JobCallbackManager.getInstance();
      registerMbean("jobCallbackJMXMBean",
          jobCallbackMgr.getJmxJobCallbackMBean());
    }
  }

  public void close() {
    try {
      for (ObjectName name : registeredMBeans) {
        mbeanServer.unregisterMBean(name);
        logger.info("Jmx MBean " + name.getCanonicalName() + " unregistered.");
      }
    } catch (Exception e) {
      logger.error("Failed to cleanup MBeanServer", e);
    }
  }

  private void registerMbean(String name, Object mbean) {
    Class<?> mbeanClass = mbean.getClass();
    ObjectName mbeanName;
    try {
      mbeanName = new ObjectName(mbeanClass.getName() + ":name=" + name);
      mbeanServer.registerMBean(mbean, mbeanName);
      logger.info("Bean " + mbeanClass.getCanonicalName() + " registered.");
      registeredMBeans.add(mbeanName);
    } catch (Exception e) {
      logger.error("Error registering mbean " + mbeanClass.getCanonicalName(),
          e);
    }

  }

  public List<ObjectName> getMbeanNames() {
    return registeredMBeans;
  }

  public MBeanInfo getMBeanInfo(ObjectName name) {
    try {
      return mbeanServer.getMBeanInfo(name);
    } catch (Exception e) {
      logger.error(e);
      return null;
    }
  }

  public Object getMBeanAttribute(ObjectName name, String attribute) {
    try {
      return mbeanServer.getAttribute(name, attribute);
    } catch (Exception e) {
      logger.error(e);
      return null;
    }
  }


  /**
   * Get the hostname
   *
   * @return hostname
   */
  public String getHost() {
    String host = "unkownHost";
    try {
      host = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception e) {
      logger.error("Failed to fetch LocalHostName");
    }
    return host;
  }

  /**
   * Get the current server port
   * @return the port at which the executor server is running
   */
  public int getPort() {
    final Connector[] connectors = server.getConnectors();
    checkState(connectors.length >= 1, "Server must have at least 1 connector");

    // The first connector is created upon initializing the server. That's the one that has the port.
    return connectors[0].getLocalPort();
  }

  /**
   * Returns host:port combination for currently running executor
   * @return
   */
  public String getExecutorHostPort() {
    return getHost() + ":" + getPort();
  }

  /**
   * Shutdown the server.
   *  - performs a safe shutdown. Waits for completion of current tasks
   *  - spawns a shutdown thread and returns immediately.
   */
  public void shutdown() {
    logger.warn("Shutting down AzkabanExecutorServer...");
    new Thread(() -> {
      try {
        // Hack: Sleep for a little time to allow API calls to complete
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        logger.error(e);
      }
      shutdownInternal();
    }, "shutdown").start();
  }

  /**
   * (internal API)
   * Note: This should be run in a separate thread.
   *
   * Shutdown the server. (blocking call)
   *  - waits for jobs to finish
   *  - doesn't accept any new jobs
   */
  private void shutdownInternal() {
    getFlowRunnerManager().shutdown();
    try {
      shutdownNow();
      logger.warn("Shutdown AzkabanExecutorServer complete");
    } catch (Exception e) {
      logger.error(e);
    }
  }

  /**
   * Shutdown the server now! (unsafe)
   * @throws Exception
   */
  public void shutdownNow() throws Exception {
    server.stop();
    server.destroy();
    SystemMemoryInfo.shutdown();
    getFlowRunnerManager().shutdownNow();
  }
}

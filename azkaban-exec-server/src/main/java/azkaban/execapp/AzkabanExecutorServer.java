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

import static azkaban.Constants.ConfigurationKeys;
import static azkaban.Constants.DEFAULT_EXECUTOR_PORT_FILE;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static azkaban.execapp.ExecJettyServerModule.EXEC_JETTY_SERVER;
import static azkaban.execapp.ExecJettyServerModule.EXEC_ROOT_CONTEXT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.execapp.event.JobCallbackManager;
import azkaban.execapp.jmx.JmxFlowRampManager;
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
import azkaban.jmx.JmxJettyServer;
import azkaban.metric.IMetricEmitter;
import azkaban.metric.MetricException;
import azkaban.metric.MetricReportManager;
import azkaban.metric.inmemoryemitter.InMemoryMetricEmitter;
import azkaban.metrics.MetricsManager;
import azkaban.server.AzkabanServer;
import azkaban.server.IMBeanRegistrable;
import azkaban.server.MBeanRegistrationManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.utils.StdOutErrRedirect;
import azkaban.utils.Utils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.time.Duration;
import java.util.TimeZone;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;


@Singleton
public class AzkabanExecutorServer implements IMBeanRegistrable {

  public static final String JOBTYPE_PLUGIN_DIR = "azkaban.jobtype.plugin.dir";
  public static final String RAMPPOLICY_PLUGIN_DIR = "azkaban.ramppolicy.plugin.dir";
  public static final String CLUSTER_CONFIG_DIR = "azkaban.cluster.dir";
  public static final String CLUSTER_ROUTER_CLASS = "azkaban.cluster.router";
  public static final String CLUSTER_ROUTER_CONF = "azkaban.cluster.router.conf";

  public static final String METRIC_INTERVAL = "executor.metric.milisecinterval.";
  private static final String CUSTOM_JMX_ATTRIBUTE_PROCESSOR_PROPERTY = "jmx.attribute.processor.class";
  private static final Logger logger = Logger.getLogger(AzkabanExecutorServer.class);

  private static AzkabanExecutorServer app;

  private final MBeanRegistrationManager mbeanRegistrationManager = new MBeanRegistrationManager();
  private final ExecutorLoader executionLoader;
  private final FlowRunnerManager runnerManager;
  private final FlowRampManager rampManager;
  private final MetricsManager metricsManager;
  private final Props props;
  private final Server server;
  private final Context root;

  @Inject
  public AzkabanExecutorServer(final Props props,
      final ExecutorLoader executionLoader,
      final FlowRunnerManager runnerManager,
      final FlowRampManager rampManager,
      final MetricsManager metricsManager,
      @Named(EXEC_JETTY_SERVER) final Server server,
      @Named(EXEC_ROOT_CONTEXT) final Context root) {
    this.props = props;
    this.executionLoader = executionLoader;
    this.runnerManager = runnerManager;
    this.rampManager = rampManager;

    this.metricsManager = metricsManager;
    this.server = server;
    this.root = root;
  }

  /**
   * Returns the currently executing executor server, if one exists.
   */
  public static AzkabanExecutorServer getApp() {
    return app;
  }

  /**
   * Azkaban using Jetty
   */
  public static void main(final String[] args) throws Exception {
    // Redirect all std out and err messages into log4j
    StdOutErrRedirect.redirectOutAndErrToLog();

    logger.info("Starting Jetty Azkaban Executor...");

    if (System.getSecurityManager() == null) {
      Policy.setPolicy(new Policy() {
        @Override
        public boolean implies(final ProtectionDomain domain, final Permission permission) {
          return true; // allow all
        }
      });
      System.setSecurityManager(new SecurityManager());
    }

    final Props props = AzkabanServer.loadProps(args);

    if (props == null) {
      logger.error("Azkaban Properties not loaded.");
      logger.error("Exiting Azkaban Executor Server...");
      return;
    }

    /* Initialize Guice Injector */
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanExecServerModule()
    );
    SERVICE_PROVIDER.setInjector(injector);

    launch(injector.getInstance(AzkabanExecutorServer.class));
  }

  public static void launch(final AzkabanExecutorServer azkabanExecutorServer) throws Exception {
    azkabanExecutorServer.start();
    setupTimeZone(azkabanExecutorServer.getAzkabanProps());
    app = azkabanExecutorServer;

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          logTopMemoryConsumers();
        } catch (final Exception e) {
          AzkabanExecutorServer.logger.info(("Exception when logging top memory consumers"), e);
        }

        final String host = AzkabanExecutorServer.app.getHost();
        final int port = AzkabanExecutorServer.app.getPort();
        try {
          AzkabanExecutorServer.logger.info(String
              .format("Removing executor(host: %s, port: %s) entry from database...", host, port));
          AzkabanExecutorServer.app.getExecutorLoader().removeExecutor(host, port);
        } catch (final ExecutorManagerException ex) {
          AzkabanExecutorServer.logger.error(
              String.format("Exception when removing executor(host: %s, port: %s)", host, port),
              ex);
        }

        AzkabanExecutorServer.logger.warn("Shutting down executor...");
        try {
          AzkabanExecutorServer.app.shutdownNow();
          AzkabanExecutorServer.app.getFlowRunnerManager().deleteExecutionDirectory();
        } catch (final Exception e) {
          AzkabanExecutorServer.logger.error("Error while shutting down http server.", e);
        }
      }

      public void logTopMemoryConsumers() throws Exception, IOException {
        if (new File("/bin/bash").exists() && new File("/bin/ps").exists()
            && new File("/usr/bin/head").exists()) {
          AzkabanExecutorServer.logger.info("logging top memory consumer");

          final java.lang.ProcessBuilder processBuilder =
              new java.lang.ProcessBuilder("/bin/bash", "-c",
                  "/bin/ps aux --sort -rss | /usr/bin/head");
          final Process p = processBuilder.start();
          p.waitFor();

          final InputStream is = p.getInputStream();
          final java.io.BufferedReader reader =
              new java.io.BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
          String line = null;
          while ((line = reader.readLine()) != null) {
            AzkabanExecutorServer.logger.info(line);
          }
          is.close();
        }
      }
    });
  }

  private static void setupTimeZone(final Props azkabanSettings) {
    if (azkabanSettings.containsKey(ConfigurationKeys.DEFAULT_TIMEZONE_ID)) {
      final String timezoneId = azkabanSettings.getString(ConfigurationKeys.DEFAULT_TIMEZONE_ID);
      System.setProperty("user.timezone", timezoneId);
      final TimeZone timeZone = TimeZone.getTimeZone(timezoneId);
      TimeZone.setDefault(timeZone);
      DateTimeZone.setDefault(DateTimeZone.forTimeZone(timeZone));
      logger.info("Setting timezone to " + timezoneId);
    }
  }

  private void start() throws Exception {
    this.root.setAttribute(Constants.AZKABAN_SERVLET_CONTEXT_KEY, this);

    JmxJobMBeanManager.getInstance().initialize(this.props);

    // make sure this happens before
    configureJobCallback(this.props);

    configureMBeanServer();
    configureMetricReports();

    loadCustomJMXAttributeProcessor(this.props);

    // Before starting, make FlowRunnerManager accept executions if active=true
    initActive();

    try {
      this.server.start();
    } catch (final Exception e) {
      logger.error(e);
      Utils.croak(e.getMessage(), 1);
    }

    insertExecutorEntryIntoDB();
    dumpPortToFile();

    logger.info("Started Executor Server on " + getExecutorHostPort());

    if (this.props.getBoolean(ConfigurationKeys.IS_METRICS_ENABLED, false)) {
      startReportingExecMetrics();
    }
  }

  private void startReportingExecMetrics() {
    logger.info("starting reporting Executor Metrics");
    this.metricsManager.startReporting("AZ-EXEC", this.props);
  }

  private void initActive() throws ExecutorManagerException {
    final Executor executor;
    final int port = this.props.getInt(ConfigurationKeys.EXECUTOR_PORT, -1);
    if (port != -1) {
      final String host = requireNonNull(getHost());
      // Check if this executor exists previously in the DB
      try {
        executor = this.executionLoader.fetchExecutor(host, port);
      } catch (final ExecutorManagerException e) {
        logger.error("Error fetching executor entry from DB", e);
        throw e;
      }
      if (executor == null) {
        logger.info("This executor wasn't found in the DB. Setting active=false.");
        getFlowRunnerManager().setActiveInternal(false);
      } else {
        logger.info("This executor is already in the DB. Found active=" + executor.isActive());
        getFlowRunnerManager().setActiveInternal(executor.isActive());
      }
    } else {
      // In case of "pick any free port" executor can't be activated based on the value in DB like above, because port
      // is only available after the jetty server has started.
      logger.info(ConfigurationKeys.EXECUTOR_PORT
          + " wasn't set - free port will be picked automatically. Executor " +
          "is started with active=false and must be activated separately.");
    }
  }

  private void insertExecutorEntryIntoDB() throws ExecutorManagerException {
    try {
      final String host = requireNonNull(getHost());
      final int port = getPort();
      checkState(port != -1);
      final Executor executor = this.executionLoader.fetchExecutor(host, port);
      if (executor == null) {
        logger.info("This executor wasn't found in the DB. Adding self.");
        this.executionLoader.addExecutor(host, port);
      } else {
        logger.info("This executor is already in the DB. Found: " + executor);
      }
      // If executor already exists, ignore it
    } catch (final ExecutorManagerException e) {
      logger.error("Error inserting executor entry into DB", e);
      throw e;
    }
  }

  private void dumpPortToFile() throws IOException {
    // By default this should write to the working directory
    final String portFileName = this.props
        .getString(ConfigurationKeys.EXECUTOR_PORT_FILE, DEFAULT_EXECUTOR_PORT_FILE);
    FileIOUtils.dumpNumberToFile(Paths.get(portFileName), getPort());
  }

  private void configureJobCallback(final Props props) {
    final boolean jobCallbackEnabled =
        props.getBoolean("azkaban.executor.jobcallback.enabled", true);

    logger.info("Job callback enabled? " + jobCallbackEnabled);

    if (jobCallbackEnabled) {
      JobCallbackManager.initialize(props);
    }
  }

  /**
   * Configure Metric Reporting as per azkaban.properties settings
   */
  private void configureMetricReports() throws MetricException {
    final Props props = getAzkabanProps();
    if (props != null && props.getBoolean("executor.metric.reports", false)) {
      logger.info("Starting to configure Metric Reports");
      final MetricReportManager metricManager = MetricReportManager.getInstance();
      final IMetricEmitter metricEmitter = new InMemoryMetricEmitter(props);
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
      metricManager.addMetric(new NumRunningFlowMetric(this.runnerManager,
          metricManager, props.getInt(METRIC_INTERVAL
              + NumRunningFlowMetric.NUM_RUNNING_FLOW_METRIC_NAME,
          props.getInt(METRIC_INTERVAL + "default"))));

      logger.info("Adding number of queued flows metric");
      metricManager.addMetric(new NumQueuedFlowMetric(this.runnerManager,
          metricManager, props.getInt(METRIC_INTERVAL
              + NumQueuedFlowMetric.NUM_QUEUED_FLOW_METRIC_NAME,
          props.getInt(METRIC_INTERVAL + "default"))));

      logger.info("Completed configuring Metric Reports");
    }

  }

  /**
   * Load a custom class, which is provided by a configuration CUSTOM_JMX_ATTRIBUTE_PROCESSOR_PROPERTY.
   * <p>
   * This method will try to instantiate an instance of this custom class and with given properties
   * as the argument in the constructor.
   * <p>
   * Basically the custom class must have a constructor that takes an argument with type
   * Properties.
   */
  private void loadCustomJMXAttributeProcessor(final Props props) {
    final String jmxAttributeEmitter =
        props.get(CUSTOM_JMX_ATTRIBUTE_PROCESSOR_PROPERTY);
    if (jmxAttributeEmitter != null) {
      try {
        logger.info("jmxAttributeEmitter: " + jmxAttributeEmitter);
        final Constructor<Props>[] constructors =
            (Constructor<Props>[]) Class.forName(jmxAttributeEmitter).getConstructors();

        constructors[0].newInstance(props.toProperties());
      } catch (final Exception e) {
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

  public ExecutorLoader getExecutorLoader() {
    return this.executionLoader;
  }

  /**
   * Returns the global azkaban properties
   */
  public Props getAzkabanProps() {
    return this.props;
  }

  public FlowRunnerManager getFlowRunnerManager() {
    return this.runnerManager;
  }

  public FlowRampManager getFlowRampManager() {
    return this.rampManager;
  }

  /**
   * Get the hostname
   *
   * @return hostname
   */
  public String getHost() {
    if (this.props.containsKey(ConfigurationKeys.AZKABAN_SERVER_HOST_NAME)) {
      final String hostName = this.props
          .getString(Constants.ConfigurationKeys.AZKABAN_SERVER_HOST_NAME);
      if (!StringUtils.isEmpty(hostName)) {
        return hostName;
      }
    }

    String host = "unkownHost";
    try {
      host = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (final Exception e) {
      logger.error("Failed to fetch LocalHostName");
    }
    return host;
  }

  /**
   * Get the current server port
   *
   * @return the port at which the executor server is running
   */
  public int getPort() {
    final Connector[] connectors = this.server.getConnectors();
    checkState(connectors.length >= 1, "Server must have at least 1 connector");

    // The first connector is created upon initializing the server. That's the one that has the port.
    return connectors[0].getLocalPort();
  }

  /**
   * Returns host:port combination for currently running executor
   */
  public String getExecutorHostPort() {
    return getHost() + ":" + getPort();
  }

  private void sleep(final Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (final InterruptedException e) {
      logger.error(e);
    }
  }

  /**
   * Shutdown the server. - performs a safe shutdown. Waits for completion of current tasks - spawns
   * a shutdown thread and returns immediately.
   */
  public void shutdown() {
    logger.warn("Shutting down AzkabanExecutorServer...");
    new Thread(() -> {
      // Hack: Sleep for a little time to allow API calls to complete
      sleep(Duration.ofSeconds(2));
      shutdownInternal();
    }, "shutdown").start();
  }

  /**
   * (internal API) Note: This should be run in a separate thread.
   * <p>
   * Shutdown the server. (blocking call) - waits for jobs to finish - doesn't accept any new jobs
   */
  private void shutdownInternal() {
    getFlowRampManager().shutdown();
    getFlowRunnerManager().shutdown();
    // Sleep for an hour to wait for web server updater thread
    // {@link azkaban.executor.RunningExecutionsUpdaterThread#updateExecutions} to finalize updating
    sleep(Duration.ofHours(1));
    // trigger shutdown hook
    System.exit(0);
  }

  /**
   * Shutdown the server now! (unsafe)
   */
  public void shutdownNow() throws Exception {
    this.server.stop();
    this.server.destroy();
    getFlowRampManager().shutdownNow();
    getFlowRunnerManager().shutdownNow();
    this.mbeanRegistrationManager.closeMBeans();
  }

  @Override
  public void configureMBeanServer() {
    logger.info("Registering MBeans...");

    this.mbeanRegistrationManager.registerMBean("executorJetty", new JmxJettyServer(this.server));
    this.mbeanRegistrationManager
        .registerMBean("flowRunnerManager", new JmxFlowRunnerManager(this.runnerManager));
    this.mbeanRegistrationManager
        .registerMBean("flowRampManager", new JmxFlowRampManager(this.rampManager));
    this.mbeanRegistrationManager.registerMBean("jobJMXMBean", JmxJobMBeanManager.getInstance());

    if (JobCallbackManager.isInitialized()) {
      final JobCallbackManager jobCallbackMgr = JobCallbackManager.getInstance();
      this.mbeanRegistrationManager
          .registerMBean("jobCallbackJMXMBean", jobCallbackMgr.getJmxJobCallbackMBean());
    }
  }

  @Override
  public MBeanRegistrationManager getMBeanRegistrationManager() {
    return this.mbeanRegistrationManager;
  }
}

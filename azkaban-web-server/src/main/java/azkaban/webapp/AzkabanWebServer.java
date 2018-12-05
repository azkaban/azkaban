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

package azkaban.webapp;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static java.util.Objects.requireNonNull;

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.jmx.JmxExecutorManager;
import azkaban.jmx.JmxJettyServer;
import azkaban.jmx.JmxTriggerManager;
import azkaban.metrics.MetricsManager;
import azkaban.project.ProjectManager;
import azkaban.scheduler.ScheduleManager;
import azkaban.server.AzkabanServer;
import azkaban.server.session.SessionCache;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerException;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.CreateTriggerAction;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.trigger.builtin.ExecutionChecker;
import azkaban.trigger.builtin.KillExecutionAction;
import azkaban.trigger.builtin.SlaAlertAction;
import azkaban.trigger.builtin.SlaChecker;
import azkaban.user.UserManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.StdOutErrRedirect;
import azkaban.utils.Utils;
import azkaban.webapp.plugin.PluginRegistry;
import azkaban.webapp.plugin.TriggerPlugin;
import azkaban.webapp.plugin.ViewerPlugin;
import azkaban.webapp.servlet.AbstractAzkabanServlet;
import azkaban.webapp.servlet.ExecutorServlet;
import azkaban.webapp.servlet.FlowTriggerInstanceServlet;
import azkaban.webapp.servlet.FlowTriggerServlet;
import azkaban.webapp.servlet.HistoryServlet;
import azkaban.webapp.servlet.IndexRedirectServlet;
import azkaban.webapp.servlet.JMXHttpServlet;
import azkaban.webapp.servlet.NoteServlet;
import azkaban.webapp.servlet.ProjectManagerServlet;
import azkaban.webapp.servlet.ProjectServlet;
import azkaban.webapp.servlet.ScheduleServlet;
import azkaban.webapp.servlet.StatsServlet;
import azkaban.webapp.servlet.StatusServlet;
import azkaban.webapp.servlet.TriggerManagerServlet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkedin.restli.server.RestliServlet;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.jmx.HierarchyDynamicMBean;
import org.apache.velocity.app.VelocityEngine;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;


/**
 * The Azkaban Jetty server class
 *
 * Global azkaban properties for setup. All of them are optional unless otherwise marked:
 * azkaban.name - The displayed name of this instance. azkaban.label - Short descriptor of this
 * Azkaban instance. azkaban.color - Theme color azkaban.temp.dir - Temp dir used by Azkaban for
 * various file uses. web.resource.dir - The directory that contains the static web files.
 * default.timezone.id - The timezone code. I.E. America/Los Angeles
 *
 * user.manager.class - The UserManager class used for the user manager. Default is XmlUserManager.
 * project.manager.class - The ProjectManager to load projects project.global.properties - The base
 * properties inherited by all projects and jobs
 *
 * jetty.maxThreads - # of threads for jetty jetty.ssl.port - The ssl port used for sessionizing.
 * jetty.keystore - Jetty keystore . jetty.keypassword - Jetty keystore password jetty.truststore -
 * Jetty truststore jetty.trustpassword - Jetty truststore password
 */
@Singleton
public class AzkabanWebServer extends AzkabanServer {

  public static final String DEFAULT_CONF_PATH = "conf";
  private static final String AZKABAN_ACCESS_LOGGER_NAME =
      "azkaban.webapp.servlet.LoginAbstractAzkabanServlet";
  private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);
  private static final int MAX_FORM_CONTENT_SIZE = 10 * 1024 * 1024;
  private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";
  private static final String DEFAULT_STATIC_DIR = "";

  @Deprecated
  private static AzkabanWebServer app;

  private final VelocityEngine velocityEngine;
  private final StatusService statusService;
  private final Server server;
  private final UserManager userManager;
  private final ProjectManager projectManager;
  private final ExecutorManagerAdapter executorManagerAdapter;
  private final ScheduleManager scheduleManager;
  private final TriggerManager triggerManager;
  private final MetricsManager metricsManager;
  private final Props props;
  private final SessionCache sessionCache;
  private final List<ObjectName> registeredMBeans = new ArrayList<>();
  private final FlowTriggerScheduler scheduler;
  private final FlowTriggerService flowTriggerService;
  private Map<String, TriggerPlugin> triggerPlugins;
  private MBeanServer mbeanServer;

  @Inject
  public AzkabanWebServer(final Props props,
      final Server server,
      final ExecutorManagerAdapter executorManagerAdapter,
      final ProjectManager projectManager,
      final TriggerManager triggerManager,
      final MetricsManager metricsManager,
      final SessionCache sessionCache,
      final UserManager userManager,
      final ScheduleManager scheduleManager,
      final VelocityEngine velocityEngine,
      final FlowTriggerScheduler scheduler,
      final FlowTriggerService flowTriggerService,
      final StatusService statusService) {
    this.props = requireNonNull(props, "props is null.");
    this.server = requireNonNull(server, "server is null.");
    this.executorManagerAdapter = requireNonNull(executorManagerAdapter,
        "executorManagerAdapter is null.");
    this.projectManager = requireNonNull(projectManager, "projectManager is null.");
    this.triggerManager = requireNonNull(triggerManager, "triggerManager is null.");
    this.metricsManager = requireNonNull(metricsManager, "metricsManager is null.");
    this.sessionCache = requireNonNull(sessionCache, "sessionCache is null.");
    this.userManager = requireNonNull(userManager, "userManager is null.");
    this.scheduleManager = requireNonNull(scheduleManager, "scheduleManager is null.");
    this.velocityEngine = requireNonNull(velocityEngine, "velocityEngine is null.");
    this.statusService = statusService;
    this.scheduler = requireNonNull(scheduler, "scheduler is null.");
    this.flowTriggerService = requireNonNull(flowTriggerService, "flow trigger service is null");

    loadBuiltinCheckersAndActions();

    // load all trigger agents here

    final String triggerPluginDir =
        props.getString("trigger.plugin.dir", "plugins/triggers");

    new PluginCheckerAndActionsLoader().load(triggerPluginDir);

    // Setup time zone
    if (props.containsKey(DEFAULT_TIMEZONE_ID)) {
      final String timezone = props.getString(DEFAULT_TIMEZONE_ID);
      System.setProperty("user.timezone", timezone);
      TimeZone.setDefault(TimeZone.getTimeZone(timezone));
      DateTimeZone.setDefault(DateTimeZone.forID(timezone));
      logger.info("Setting timezone to " + timezone);
    }
    configureMBeanServer();
  }

  @Deprecated
  public static AzkabanWebServer getInstance() {
    return app;
  }

  public static void main(final String[] args) throws Exception {
    // Redirect all std out and err messages into log4j
    StdOutErrRedirect.redirectOutAndErrToLog();

    logger.info("Starting Jetty Azkaban Web Server...");
    final Props props = AzkabanServer.loadProps(args);

    if (props == null) {
      logger.error("Azkaban Properties not loaded. Exiting..");
      System.exit(1);
    }

    /* Initialize Guice Injector */
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(props)
    );
    SERVICE_PROVIDER.setInjector(injector);

    launch(injector.getInstance(AzkabanWebServer.class));
  }

  public static void launch(final AzkabanWebServer webServer) throws Exception {
    /* This creates the Web Server instance */
    app = webServer;

    // Todo jamiesjc: also start the threads in ExecutionController if needed.
    if (webServer.executorManagerAdapter instanceof ExecutorManager) {
      ((ExecutorManager) webServer.executorManagerAdapter).start();
    }

    // TODO refactor code into ServerProvider
    webServer.prepareAndStartServer();

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          if (webServer.props.getBoolean(ConfigurationKeys.ENABLE_QUARTZ, false)) {
            logger.info("Shutting down flow trigger scheduler...");
            webServer.scheduler.shutdown();
          }
        } catch (final Exception e) {
          logger.error("Exception while shutting down flow trigger service.", e);
        }

        try {
          if (webServer.props.getBoolean(ConfigurationKeys.ENABLE_QUARTZ, false)) {
            logger.info("Shutting down flow trigger service...");
            webServer.flowTriggerService.shutdown();
          }
        } catch (final Exception e) {
          logger.error("Exception while shutting down flow trigger service.", e);
        }

        try {
          logger.info("Logging top memory consumers...");
          logTopMemoryConsumers();

          logger.info("Shutting down http server...");
          webServer.close();

        } catch (final Exception e) {
          logger.error("Exception while shutting down web server.", e);
        }

        logger.info("kk thx bye.");
      }

      public void logTopMemoryConsumers() throws Exception {
        if (new File("/bin/bash").exists() && new File("/bin/ps").exists()
            && new File("/usr/bin/head").exists()) {
          logger.info("logging top memory consumer");

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
            logger.info(line);
          }
          is.close();
        }
      }
    });
  }

  private static void loadViewerPlugins(final Context root, final String pluginPath,
      final VelocityEngine ve) {
    final File viewerPluginPath = new File(pluginPath);
    if (!viewerPluginPath.exists()) {
      return;
    }

    final ClassLoader parentLoader = AzkabanWebServer.class.getClassLoader();
    final File[] pluginDirs = viewerPluginPath.listFiles();
    final ArrayList<String> jarPaths = new ArrayList<>();
    for (final File pluginDir : pluginDirs) {
      if (!pluginDir.exists()) {
        logger.error("Error viewer plugin path " + pluginDir.getPath()
            + " doesn't exist.");
        continue;
      }

      if (!pluginDir.isDirectory()) {
        logger.error("The plugin path " + pluginDir + " is not a directory.");
        continue;
      }

      // Load the conf directory
      final File propertiesDir = new File(pluginDir, "conf");
      Props pluginProps = null;
      if (propertiesDir.exists() && propertiesDir.isDirectory()) {
        final File propertiesFile = new File(propertiesDir, "plugin.properties");
        final File propertiesOverrideFile =
            new File(propertiesDir, "override.properties");

        if (propertiesFile.exists()) {
          if (propertiesOverrideFile.exists()) {
            pluginProps =
                PropsUtils.loadProps(null, propertiesFile,
                    propertiesOverrideFile);
          } else {
            pluginProps = PropsUtils.loadProps(null, propertiesFile);
          }
        } else {
          logger.error("Plugin conf file " + propertiesFile + " not found.");
          continue;
        }
      } else {
        logger.error("Plugin conf path " + propertiesDir + " not found.");
        continue;
      }

      final String pluginName = pluginProps.getString("viewer.name");
      final String pluginWebPath = pluginProps.getString("viewer.path");
      final String pluginJobTypes = pluginProps.getString("viewer.jobtypes", null);
      final int pluginOrder = pluginProps.getInt("viewer.order", 0);
      final boolean pluginHidden = pluginProps.getBoolean("viewer.hidden", false);
      final List<String> extLibClasspath =
          pluginProps.getStringList("viewer.external.classpaths",
              (List<String>) null);

      final String pluginClass = pluginProps.getString("viewer.servlet.class");
      if (pluginClass == null) {
        logger.error("Viewer class is not set.");
      } else {
        logger.info("Plugin class " + pluginClass);
      }

      URLClassLoader urlClassLoader = null;
      final File libDir = new File(pluginDir, "lib");
      if (libDir.exists() && libDir.isDirectory()) {
        final File[] files = libDir.listFiles();

        final ArrayList<URL> urls = new ArrayList<>();
        for (int i = 0; i < files.length; ++i) {
          try {
            final URL url = files[i].toURI().toURL();
            urls.add(url);
          } catch (final MalformedURLException e) {
            logger.error(e);
          }
        }

        // Load any external libraries.
        if (extLibClasspath != null) {
          for (final String extLib : extLibClasspath) {
            final File extLibFile = new File(pluginDir, extLib);
            if (extLibFile.exists()) {
              if (extLibFile.isDirectory()) {
                // extLibFile is a directory; load all the files in the
                // directory.
                final File[] extLibFiles = extLibFile.listFiles();
                for (int i = 0; i < extLibFiles.length; ++i) {
                  try {
                    final URL url = extLibFiles[i].toURI().toURL();
                    urls.add(url);
                  } catch (final MalformedURLException e) {
                    logger.error(e);
                  }
                }
              } else { // extLibFile is a file
                try {
                  final URL url = extLibFile.toURI().toURL();
                  urls.add(url);
                } catch (final MalformedURLException e) {
                  logger.error(e);
                }
              }
            } else {
              logger.error("External library path "
                  + extLibFile.getAbsolutePath() + " not found.");
              continue;
            }
          }
        }

        urlClassLoader =
            new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
      } else {
        logger
            .error("Library path " + libDir.getAbsolutePath() + " not found.");
        continue;
      }

      Class<?> viewerClass = null;
      try {
        viewerClass = urlClassLoader.loadClass(pluginClass);
      } catch (final ClassNotFoundException e) {
        logger.error("Class " + pluginClass + " not found.");
        continue;
      }

      final String source = FileIOUtils.getSourcePathFromClass(viewerClass);
      logger.info("Source jar " + source);
      jarPaths.add("jar:file:" + source);

      Constructor<?> constructor = null;
      try {
        constructor = viewerClass.getConstructor(Props.class);
      } catch (final NoSuchMethodException e) {
        logger.error("Constructor not found in " + pluginClass);
        continue;
      }

      Object obj = null;
      try {
        obj = constructor.newInstance(pluginProps);
      } catch (final Exception e) {
        logger.error(e);
        logger.error(e.getCause());
      }

      if (!(obj instanceof AbstractAzkabanServlet)) {
        logger.error("The object is not an AbstractAzkabanServlet");
        continue;
      }

      final AbstractAzkabanServlet avServlet = (AbstractAzkabanServlet) obj;
      root.addServlet(new ServletHolder(avServlet), "/" + pluginWebPath + "/*");
      PluginRegistry.getRegistry().register(
          new ViewerPlugin(pluginName, pluginWebPath, pluginOrder,
              pluginHidden, pluginJobTypes));
    }

    // Velocity needs the jar resource paths to be set.
    final String jarResourcePath = StringUtils.join(jarPaths, ", ");
    logger.info("Setting jar resource path " + jarResourcePath);
    ve.addProperty("jar.resource.loader.path", jarResourcePath);
  }

  public FlowTriggerService getFlowTriggerService() {
    return this.flowTriggerService;
  }

  public FlowTriggerScheduler getScheduler() {
    return this.scheduler;
  }

  private void validateDatabaseVersion()
      throws IOException, SQLException {
    final boolean checkDB = this.props
        .getBoolean(AzkabanDatabaseSetup.DATABASE_CHECK_VERSION, false);
    if (checkDB) {
      final AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(this.props);
      setup.loadTableInfo();
      if (setup.needsUpdating()) {
        logger.error("Database is out of date.");
        setup.printUpgradePlan();

        logger.error("Exiting with error.");
        System.exit(-1);
      }
    }
  }

  private void configureRoutes() throws TriggerManagerException {
    final String staticDir =
        this.props.getString("web.resource.dir", DEFAULT_STATIC_DIR);
    logger.info("Setting up web resource dir " + staticDir);
    final Context root = new Context(this.server, "/", Context.SESSIONS);
    root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);

    final String defaultServletPath =
        this.props.getString("azkaban.default.servlet.path", "/index");
    root.setResourceBase(staticDir);
    final ServletHolder indexRedirect =
        new ServletHolder(new IndexRedirectServlet(defaultServletPath));
    root.addServlet(indexRedirect, "/");
    final ServletHolder index = new ServletHolder(new ProjectServlet());
    root.addServlet(index, "/index");

    final ServletHolder staticServlet = new ServletHolder(new DefaultServlet());
    root.addServlet(staticServlet, "/css/*");
    root.addServlet(staticServlet, "/js/*");
    root.addServlet(staticServlet, "/images/*");
    root.addServlet(staticServlet, "/fonts/*");
    root.addServlet(staticServlet, "/favicon.ico");

    root.addServlet(new ServletHolder(new ProjectManagerServlet()), "/manager");
    root.addServlet(new ServletHolder(new ExecutorServlet()), "/executor");
    root.addServlet(new ServletHolder(new HistoryServlet()), "/history");
    root.addServlet(new ServletHolder(new ScheduleServlet()), "/schedule");
    root.addServlet(new ServletHolder(new JMXHttpServlet()), "/jmx");
    root.addServlet(new ServletHolder(new TriggerManagerServlet()), "/triggers");
    root.addServlet(new ServletHolder(new StatsServlet()), "/stats");
    root.addServlet(new ServletHolder(new StatusServlet(this.statusService)), "/status");
    root.addServlet(new ServletHolder(new NoteServlet()), "/notes");
    root.addServlet(new ServletHolder(new FlowTriggerInstanceServlet()), "/flowtriggerinstance");
    root.addServlet(new ServletHolder(new FlowTriggerServlet()), "/flowtrigger");

    final ServletHolder restliHolder = new ServletHolder(new RestliServlet());
    restliHolder.setInitParameter("resourcePackages", "azkaban.restli");
    root.addServlet(restliHolder, "/restli/*");

    final String viewerPluginDir =
        this.props.getString("viewer.plugin.dir", "plugins/viewer");
    loadViewerPlugins(root, viewerPluginDir, getVelocityEngine());

    // Trigger Plugin Loader
    final TriggerPluginLoader triggerPluginLoader = new TriggerPluginLoader(this.props);

    final Map<String, TriggerPlugin> triggerPlugins = triggerPluginLoader.loadTriggerPlugins(root);
    setTriggerPlugins(triggerPlugins);
    // always have basic time trigger
    // TODO: find something else to do the job
    getTriggerManager().start();

    root.setAttribute(Constants.AZKABAN_SERVLET_CONTEXT_KEY, this);
  }

  private void prepareAndStartServer()
      throws Exception {
    validateDatabaseVersion();
    createThreadPool();
    configureRoutes();

    // Todo jamiesjc: enable web metrics for azkaban poll model later
    if (this.props.getBoolean(Constants.ConfigurationKeys.IS_METRICS_ENABLED, false)
        && !this.props.getBoolean(ConfigurationKeys.AZKABAN_POLL_MODEL, false)) {
      startWebMetrics();
    }

    if (this.props.getBoolean(ConfigurationKeys.ENABLE_QUARTZ, false)) {
      // flowTriggerService needs to be started first before scheduler starts to schedule
      // existing flow triggers
      logger.info("starting flow trigger service");
      this.flowTriggerService.start();
      logger.info("starting flow trigger scheduler");
      this.scheduler.start();
    }

    try {
      this.server.start();
      logger.info("Server started");
    } catch (final Exception e) {
      logger.warn(e);
      Utils.croak(e.getMessage(), 1);
    }
  }

  private void createThreadPool() {
    final int maxThreads = this.props
        .getInt("jetty.maxThreads", Constants.DEFAULT_JETTY_MAX_THREAD_COUNT);

    final QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
    this.server.setThreadPool(httpThreadPool);
    addThreadPoolGauges(httpThreadPool);
  }

  private void addThreadPoolGauges(final QueuedThreadPool threadPool) {
    // The number of idle threads in Jetty thread pool
    this.metricsManager.addGauge("JETTY-NumIdleThreads", threadPool::getIdleThreads);

    // The number of threads in Jetty thread pool. The formula is:
    // threads = idleThreads + busyThreads
    this.metricsManager.addGauge("JETTY-NumTotalThreads", threadPool::getThreads);

    // The number of requests queued in the Jetty thread pool.
    this.metricsManager.addGauge("JETTY-NumQueueSize", threadPool::getQueueSize);
  }


  private void startWebMetrics() throws Exception {
    this.metricsManager
        .addGauge("WEB-NumQueuedFlows", ((ExecutorManager) this
            .executorManagerAdapter)::getQueuedFlowSize);
    /*
     * TODO: Currently {@link ExecutorManager#getRunningFlows()} includes both running and non-dispatched flows.
     * Originally we would like to do a subtraction between getRunningFlows and {@link ExecutorManager#getQueuedFlowSize()},
     * in order to have the correct runnable flows.
     * However, both getRunningFlows and getQueuedFlowSize are not synchronized, such that we can not make
     * a thread safe subtraction. We need to fix this in the future.
     */
    this.metricsManager
        .addGauge("WEB-NumRunningFlows",
            () -> ((ExecutorManager) this.executorManagerAdapter).getRunningFlows().size());

    logger.info("starting reporting Web Server Metrics");
    this.metricsManager.startReporting("AZ-WEB", this.props);
  }

  private void loadBuiltinCheckersAndActions() {
    logger.info("Loading built-in checker and action types");
    ExecuteFlowAction.setExecutorManager(this.executorManagerAdapter);
    ExecuteFlowAction.setProjectManager(this.projectManager);
    ExecuteFlowAction.setTriggerManager(this.triggerManager);
    KillExecutionAction.setExecutorManager(this.executorManagerAdapter);
    CreateTriggerAction.setTriggerManager(this.triggerManager);
    ExecutionChecker.setExecutorManager(this.executorManagerAdapter);

    this.triggerManager.registerCheckerType(BasicTimeChecker.type, BasicTimeChecker.class);
    this.triggerManager.registerCheckerType(SlaChecker.type, SlaChecker.class);
    this.triggerManager.registerCheckerType(ExecutionChecker.type, ExecutionChecker.class);
    this.triggerManager.registerActionType(ExecuteFlowAction.type, ExecuteFlowAction.class);
    this.triggerManager.registerActionType(KillExecutionAction.type, KillExecutionAction.class);
    this.triggerManager.registerActionType(SlaAlertAction.type, SlaAlertAction.class);
    this.triggerManager.registerActionType(CreateTriggerAction.type, CreateTriggerAction.class);
  }

  /**
   * Returns the web session cache.
   */
  @Override
  public SessionCache getSessionCache() {
    return this.sessionCache;
  }

  /**
   * Returns the velocity engine for pages to use.
   */
  @Override
  public VelocityEngine getVelocityEngine() {
    return this.velocityEngine;
  }

  @Override
  public UserManager getUserManager() {
    return this.userManager;
  }

  public ProjectManager getProjectManager() {
    return this.projectManager;
  }

  public ExecutorManagerAdapter getExecutorManager() {
    return this.executorManagerAdapter;
  }

  public ScheduleManager getScheduleManager() {
    return this.scheduleManager;
  }

  public TriggerManager getTriggerManager() {
    return this.triggerManager;
  }

  /**
   * Returns the global azkaban properties
   */
  @Override
  public Props getServerProps() {
    return this.props;
  }

  public Map<String, TriggerPlugin> getTriggerPlugins() {
    return this.triggerPlugins;
  }

  private void setTriggerPlugins(final Map<String, TriggerPlugin> triggerPlugins) {
    this.triggerPlugins = triggerPlugins;
  }

  private void configureMBeanServer() {
    logger.info("Registering MBeans...");
    this.mbeanServer = ManagementFactory.getPlatformMBeanServer();

    registerMbean("jetty", new JmxJettyServer(this.server));
    registerMbean("triggerManager", new JmxTriggerManager(this.triggerManager));
    // Todo jamiesjc: enable Jmx for executionController later
    if (this.executorManagerAdapter != null
        && this.executorManagerAdapter instanceof ExecutorManager) {
      registerMbean("executorManager",
          new JmxExecutorManager((ExecutorManager) this.executorManagerAdapter));
    }

    // Register Log4J loggers as JMX beans so the log level can be
    // updated via JConsole or Java VisualVM
    final HierarchyDynamicMBean log4jMBean = new HierarchyDynamicMBean();
    registerMbean("log4jmxbean", log4jMBean);
    final ObjectName accessLogLoggerObjName =
        log4jMBean.addLoggerMBean(AZKABAN_ACCESS_LOGGER_NAME);

    if (accessLogLoggerObjName == null) {
      logger.info(
          "************* loginLoggerObjName is null, make sure there is a logger with name "
              + AZKABAN_ACCESS_LOGGER_NAME);
    } else {
      logger.info("******** loginLoggerObjName: "
          + accessLogLoggerObjName.getCanonicalName());
    }
  }

  public void close() {
    try {
      for (final ObjectName name : this.registeredMBeans) {
        this.mbeanServer.unregisterMBean(name);
        logger.info("Jmx MBean " + name.getCanonicalName() + " unregistered.");
      }
    } catch (final Exception e) {
      logger.error("Failed to cleanup MBeanServer", e);
    }
    this.scheduleManager.shutdown();
    this.executorManagerAdapter.shutdown();
    try {
      this.server.stop();
    } catch (final Exception e) {
      // Catch all while closing server
      logger.error(e);
    }
    this.server.destroy();
  }

  private void registerMbean(final String name, final Object mbean) {
    final Class<?> mbeanClass = mbean.getClass();
    final ObjectName mbeanName;
    try {
      mbeanName = new ObjectName(mbeanClass.getName() + ":name=" + name);
      this.mbeanServer.registerMBean(mbean, mbeanName);
      logger.info("Bean " + mbeanClass.getCanonicalName() + " registered.");
      this.registeredMBeans.add(mbeanName);
    } catch (final Exception e) {
      logger.error("Error registering mbean " + mbeanClass.getCanonicalName(),
          e);
    }
  }

  public List<ObjectName> getMbeanNames() {
    return this.registeredMBeans;
  }

  public MBeanInfo getMBeanInfo(final ObjectName name) {
    try {
      return this.mbeanServer.getMBeanInfo(name);
    } catch (final Exception e) {
      logger.error(e);
      return null;
    }
  }

  public Object getMBeanAttribute(final ObjectName name, final String attribute) {
    try {
      return this.mbeanServer.getAttribute(name, attribute);
    } catch (final Exception e) {
      logger.error(e);
      return null;
    }
  }
}

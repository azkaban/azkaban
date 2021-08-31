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

import static azkaban.Constants.AZKABAN_SERVLET_CONTEXT_KEY;
import static azkaban.Constants.ConfigurationKeys.DEFAULT_TIMEZONE_ID;
import static azkaban.Constants.ConfigurationKeys.ENABLE_QUARTZ;
import static azkaban.Constants.MAX_FORM_CONTENT_SIZE;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static java.util.Objects.requireNonNull;

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.executor.ExecutionController;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.imagemgmt.permission.PermissionManager;
import azkaban.imagemgmt.services.ImageMgmtCommonService;
import azkaban.imagemgmt.services.ImageRampupService;
import azkaban.imagemgmt.services.ImageTypeService;
import azkaban.imagemgmt.services.ImageVersionService;
import azkaban.imagemgmt.servlets.ImageRampupServlet;
import azkaban.imagemgmt.servlets.ImageTypeServlet;
import azkaban.imagemgmt.servlets.ImageVersionServlet;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.jmx.JmxContainerizedDispatchManager;
import azkaban.jmx.JmxExecutionController;
import azkaban.jmx.JmxExecutorManager;
import azkaban.jmx.JmxJettyServer;
import azkaban.jmx.JmxTriggerManager;
import azkaban.metrics.AzkabanAPIMetrics;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.project.ProjectManager;
import azkaban.scheduler.ScheduleManager;
import azkaban.server.AzkabanAPI;
import azkaban.server.AzkabanServer;
import azkaban.server.IMBeanRegistrable;
import azkaban.server.MBeanRegistrationManager;
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
import azkaban.utils.PluginUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.StdOutErrRedirect;
import azkaban.utils.Utils;
import azkaban.webapp.metrics.WebMetrics;
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
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.NoteServlet;
import azkaban.webapp.servlet.ProjectManagerServlet;
import azkaban.webapp.servlet.ProjectServlet;
import azkaban.webapp.servlet.ScheduleServlet;
import azkaban.webapp.servlet.StatsServlet;
import azkaban.webapp.servlet.StatusServlet;
import azkaban.webapp.servlet.TriggerManagerServlet;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkedin.restli.server.RestliServlet;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.management.ObjectName;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.jmx.HierarchyDynamicMBean;
import org.apache.velocity.app.VelocityEngine;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

/**
 * The Azkaban Jetty server class
 * <p>
 * Global azkaban properties for setup. All of them are optional unless otherwise marked:
 * azkaban.name - The displayed name of this instance. azkaban.label - Short descriptor of this
 * Azkaban instance. azkaban.color - Theme color azkaban.temp.dir - Temp dir used by Azkaban for
 * various file uses. web.resource.dir - The directory that contains the static web files.
 * default.timezone.id - The timezone code. I.E. America/Los Angeles
 * <p>
 * user.manager.class - The UserManager class used for the user manager. Default is XmlUserManager.
 * project.manager.class - The ProjectManager to load projects project.global.properties - The base
 * properties inherited by all projects and jobs
 * <p>
 * jetty.maxThreads - # of threads for jetty jetty.ssl.port - The ssl port used for sessionizing.
 * jetty.keystore - Jetty keystore . jetty.keypassword - Jetty keystore password jetty.truststore -
 * Jetty truststore jetty.trustpassword - Jetty truststore password
 */
@Singleton
public class AzkabanWebServer extends AzkabanServer implements IMBeanRegistrable {

  private static final String AZKABAN_ACCESS_LOGGER_NAME =
      "azkaban.webapp.servlet.LoginAbstractAzkabanServlet";
  private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);

  @Deprecated
  private static AzkabanWebServer app;

  private final MBeanRegistrationManager mbeanRegistrationManager = new MBeanRegistrationManager();
  private final VelocityEngine velocityEngine;
  private final StatusService statusService;
  private final Server server;
  private final UserManager userManager;
  private final ProjectManager projectManager;
  private final ExecutorManagerAdapter executorManagerAdapter;
  private final ScheduleManager scheduleManager;
  private final TriggerManager triggerManager;
  private final WebMetrics webMetrics;
  private final Props props;
  private final SessionCache sessionCache;
  private final FlowTriggerScheduler flowTriggerScheduler;
  private final FlowTriggerService flowTriggerService;
  private Map<String, TriggerPlugin> triggerPlugins;
  private final ExecutionLogsCleaner executionLogsCleaner;
  private final ObjectMapper objectMapper;
  private final ContainerizationMetrics containerizationMetrics;

  @Inject
  public AzkabanWebServer(final Props props,
      final Server server,
      final ExecutorManagerAdapter executorManagerAdapter,
      final ProjectManager projectManager,
      final TriggerManager triggerManager,
      final WebMetrics webMetrics,
      final SessionCache sessionCache,
      final UserManager userManager,
      final ScheduleManager scheduleManager,
      final VelocityEngine velocityEngine,
      final FlowTriggerScheduler flowTriggerScheduler,
      final FlowTriggerService flowTriggerService,
      final StatusService statusService,
      final ExecutionLogsCleaner executionLogsCleaner,
      final ObjectMapper objectMapper,
      final ContainerizationMetrics containerizationMetrics) {
    this.props = requireNonNull(props, "props is null.");
    this.server = requireNonNull(server, "server is null.");
    this.executorManagerAdapter = requireNonNull(executorManagerAdapter,
        "executorManagerAdapter is null.");
    this.projectManager = requireNonNull(projectManager, "projectManager is null.");
    this.triggerManager = requireNonNull(triggerManager, "triggerManager is null.");
    this.webMetrics = requireNonNull(webMetrics, "webMetrics is null.");
    this.sessionCache = requireNonNull(sessionCache, "sessionCache is null.");
    this.userManager = requireNonNull(userManager, "userManager is null.");
    this.scheduleManager = requireNonNull(scheduleManager, "scheduleManager is null.");
    this.velocityEngine = requireNonNull(velocityEngine, "velocityEngine is null.");
    this.statusService = statusService;
    this.flowTriggerScheduler = requireNonNull(flowTriggerScheduler, "scheduler is null.");
    this.flowTriggerService = requireNonNull(flowTriggerService, "flow trigger service is null");
    this.executionLogsCleaner = requireNonNull(executionLogsCleaner, "executionlogcleaner is null");
    this.objectMapper = objectMapper;
    this.containerizationMetrics = containerizationMetrics;

    loadBuiltinCheckersAndActions();

    // For flow restart in EXECUTION_STOPPED state due to infra failure
    ExecutionControllerUtils.setProjectManager(this.projectManager);
    ExecutionControllerUtils.setExecutorManagerAdapter(this.executorManagerAdapter);

    // load all trigger agents here

    final String triggerPluginDir =
        props.getString("trigger.plugin.dir", "plugins/triggers");

    new PluginCheckerAndActionsLoader().load(triggerPluginDir);

    // Setup time zone
    if (props.containsKey(DEFAULT_TIMEZONE_ID)) {
      final String timezoneId = props.getString(DEFAULT_TIMEZONE_ID);
      System.setProperty("user.timezone", timezoneId);
      final TimeZone timeZone = TimeZone.getTimeZone(timezoneId);
      TimeZone.setDefault(timeZone);
      DateTimeZone.setDefault(DateTimeZone.forTimeZone(timeZone));
      logger.info("Setting timezone to " + timezoneId);
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

    webServer.validateDatabaseVersion();

    webServer.prepareAndStartServer();

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          if (webServer.props.getBoolean(ENABLE_QUARTZ, false)) {
            AzkabanWebServer.logger.info("Shutting down flow trigger scheduler...");
            webServer.flowTriggerScheduler.shutdown();
          }
        } catch (final Exception e) {
          AzkabanWebServer.logger.error("Exception while shutting down flow trigger service.", e);
        }

        try {
          if (webServer.props.getBoolean(ENABLE_QUARTZ, false)) {
            AzkabanWebServer.logger.info("Shutting down flow trigger service...");
            webServer.flowTriggerService.shutdown();
          }
        } catch (final Exception e) {
          AzkabanWebServer.logger.error("Exception while shutting down flow trigger service.", e);
        }

        try {
          AzkabanWebServer.logger.info("Logging top memory consumers...");
          logTopMemoryConsumers();

          AzkabanWebServer.logger.info("Shutting down http server...");
          webServer.close();

        } catch (final Exception e) {
          AzkabanWebServer.logger.error("Exception while shutting down web server.", e);
        }

        AzkabanWebServer.logger.info("kk thx bye.");
      }

      public void logTopMemoryConsumers() throws Exception {
        if (new File("/bin/bash").exists() && new File("/bin/ps").exists()
            && new File("/usr/bin/head").exists()) {
          AzkabanWebServer.logger.info("logging top memory consumer");

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
            AzkabanWebServer.logger.info(line);
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
      // load plugin properties
      final Props pluginProps = PropsUtils.loadPluginProps(pluginDir);
      if (pluginProps == null) {
        continue;
      }

      final String pluginName = pluginProps.getString("viewer.name");
      final String pluginWebPath = pluginProps.getString("viewer.path");
      final String pluginJobTypes = pluginProps.getString("viewer.jobtypes", null);
      final int pluginOrder = pluginProps.getInt("viewer.order", 0);
      final boolean pluginHidden = pluginProps.getBoolean("viewer.hidden", false);
      final List<String> extLibClassPaths =
          pluginProps.getStringList("viewer.external.classpaths",
              (List<String>) null);

      final String pluginClass = pluginProps.getString("viewer.servlet.class");
      if (pluginClass == null) {
        logger.error("Viewer class is not set.");
        continue;
      } else {
        logger.info("Plugin class " + pluginClass);
      }

      final Class<?> viewerClass =
          PluginUtils.getPluginClass(pluginClass, pluginDir, extLibClassPaths, parentLoader);
      if (viewerClass == null) {
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
    // ...but only when path is not empty! https://github.com/azkaban/azkaban/issues/917
    if (!jarPaths.isEmpty()) {
      final String jarResourcePath = StringUtils.join(jarPaths, ", ");
      logger.info("Setting jar resource path " + jarResourcePath);
      ve.addProperty("jar.resource.loader.path", jarResourcePath);
    }
  }

  public FlowTriggerService getFlowTriggerService() {
    return this.flowTriggerService;
  }

  public FlowTriggerScheduler getFlowTriggerScheduler() {
    return this.flowTriggerScheduler;
  }

  private void validateDatabaseVersion() throws IOException, SQLException {
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
    final Context root = new Context(this.server, "/", Context.SESSIONS);
    root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);
    root.setAttribute(AZKABAN_SERVLET_CONTEXT_KEY, this);

    final String staticDir = this.props.getString("web.resource.dir", "");
    logger.info("Setting up web resource dir " + staticDir);
    root.setResourceBase(staticDir);

    final ServletHolder staticServlet = new ServletHolder(new DefaultServlet());
    root.addServlet(staticServlet, "/css/*");
    root.addServlet(staticServlet, "/js/*");
    root.addServlet(staticServlet, "/images/*");
    root.addServlet(staticServlet, "/fonts/*");
    root.addServlet(staticServlet, "/favicon.ico");

    final Map<String, AbstractAzkabanServlet> routesMap = getRoutesMap();

    // Configure core routes
    for (final Entry<String, AbstractAzkabanServlet> entry : routesMap.entrySet()) {
      root.addServlet(new ServletHolder(entry.getValue()), entry.getKey());
    }

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

    // Set up api endpoint metrics
    // At the moment login action doesn't have a dedicated route, any route can be used to
    // authenticate when passing the right parameters. The same metrics object is used for login
    // requests on every route.
    final AzkabanAPIMetrics loginAPIMetrics = this.webMetrics.setUpAzkabanAPIMetrics(
        "_action-" + LoginAbstractAzkabanServlet.getLoginAPI().getParameterValue());
    LoginAbstractAzkabanServlet.getLoginAPI().setMetrics(loginAPIMetrics);
    for (final Entry<String, AbstractAzkabanServlet> entry : routesMap.entrySet()) {
      final List<AzkabanAPI> servletApiEndpoints = entry.getValue().getApiEndpoints();
      for (final AzkabanAPI api : servletApiEndpoints) {
        final String uri = entry.getKey().replace("/", "") + "_" + api.getRequestParameter() +
            (api.getParameterValue().isEmpty() ? "" : "-" + api.getParameterValue());
        api.setMetrics(this.webMetrics.setUpAzkabanAPIMetrics(uri));
      }
    }

    // Configure api metrics filter
    final FilterHolder metricsFilter = new FilterHolder(new APIMetricsFilter(routesMap));
    final FilterMapping metricsFilterMapping = new FilterMapping();
    metricsFilterMapping.setFilterName(metricsFilter.getName());
    final String[] servletPaths = routesMap.keySet().stream().toArray(String[]::new);
    metricsFilterMapping.setPathSpecs(servletPaths);
    metricsFilterMapping.setDispatches(Handler.REQUEST);
    root.getServletHandler().addFilter(metricsFilter, metricsFilterMapping);
  }

  /**
   * @return The routing map of path to servlets.
   */
  @VisibleForTesting
  protected Map<String, AbstractAzkabanServlet> getRoutesMap() {
    final String defaultServletPath =
        this.props.getString("azkaban.default.servlet.path", "/index");

    final Map<String, AbstractAzkabanServlet> routesMap = new HashMap<>();
    routesMap.put("/index", new ProjectServlet());
    routesMap.put("/manager", new ProjectManagerServlet());
    routesMap.put("/executor", new ExecutorServlet());
    routesMap.put("/schedule", new ScheduleServlet());
    routesMap.put("/triggers", new TriggerManagerServlet());
    routesMap.put("/flowtrigger", new FlowTriggerServlet());
    routesMap.put("/flowtriggerinstance", new FlowTriggerInstanceServlet());
    routesMap.put("/history", new HistoryServlet());
    routesMap.put("/jmx", new JMXHttpServlet());
    routesMap.put("/stats", new StatsServlet());
    routesMap.put("/notes", new NoteServlet());
    routesMap.put("/", new IndexRedirectServlet(defaultServletPath));

    routesMap.put("/status", new StatusServlet(this.statusService));

    if (isContainerizedDispatchMethodEnabled()) {
      routesMap.put("/imageTypes/*", new ImageTypeServlet());
      routesMap.put("/imageVersions/*", new ImageVersionServlet());
      routesMap.put("/imageRampup/*", new ImageRampupServlet());
    }
    return routesMap;
  }

  /**
   * @return True if the Containerization is enabled, otherwise false.
   */
  private boolean isContainerizedDispatchMethodEnabled() {
    return DispatchMethod.isContainerizedMethodEnabled(this.props
        .getString(Constants.ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
            DispatchMethod.PUSH.name()));
  }

  private void prepareAndStartServer() throws Exception {
    this.executorManagerAdapter.start();
    this.executionLogsCleaner.start();

    configureRoutes();
    startWebMetrics();
    startContainerMetrics();


    if (this.props.getBoolean(ENABLE_QUARTZ, false)) {
      // flowTriggerService needs to be started first before scheduler starts to schedule
      // existing flow triggers
      logger.info("Starting flow trigger service");
      this.flowTriggerService.start();
      logger.info("Starting flow trigger scheduler");
      this.flowTriggerScheduler.start();
    }

    try {
      this.server.start();
      logger.info("Server started");
    } catch (final Exception e) {
      logger.warn(e);
      Utils.croak(e.getMessage(), 1);
    }
  }

  private void startWebMetrics() {
    final QueuedThreadPool queuedThreadPool = (QueuedThreadPool) this.server.getThreadPool();
    final ExecutorManagerAdapter executorManagerAdapter = this.executorManagerAdapter;
    final SessionCache sessionCache = this.sessionCache;

    final int minAgeForClassifyingAFlowAged = this.props.getInt(
        ConfigurationKeys.MIN_AGE_FOR_CLASSIFYING_A_FLOW_AGED_MINUTES,
        Constants.DEFAULT_MIN_AGE_FOR_CLASSIFYING_A_FLOW_AGED_MINUTES);
    if (minAgeForClassifyingAFlowAged < 0) {
      logger.error(String.format("Property config file contains a value of %d for %s. "
              + "Metric NumAgedQueuedFlows is emitted only when this value is non-negative.",
          minAgeForClassifyingAFlowAged,
          ConfigurationKeys.MIN_AGE_FOR_CLASSIFYING_A_FLOW_AGED_MINUTES));
    }

    this.webMetrics.setUp(new WebMetrics.DataProvider() {

      @Override
      public int getNumberOfIdleServerThreads() {
        // The number of idle threads in Jetty thread pool
        return queuedThreadPool.getIdleThreads();
      }

      @Override
      public int getNumberOfServerThreads() {
        // The number of threads in Jetty thread pool. The formula is:
        // threads = idleThreads + busyThreads
        return queuedThreadPool.getThreads();
      }

      @Override
      public int getServerJobsQueueSize() {
        // The number of requests queued in the Jetty thread pool.
        return queuedThreadPool.getQueueSize();
      }

      @Override
      public long getNumberOfQueuedFlows() {
        return executorManagerAdapter.getQueuedFlowSize();
      }

      @Override
      public int getNumberOfRunningFlows() {
        /*
         * TODO: Currently {@link ExecutorManager#getRunningFlows()} includes both running and
         * non-dispatched flows. Originally we would like to do a subtraction between
         * getRunningFlows and {@link ExecutorManager#getQueuedFlowSize()}, in order to have the
         * correct runnable flows. However, both getRunningFlows and getQueuedFlowSize are not
         * synchronized, such that we can not make a thread safe subtraction. We need to fix this
         *  in the future.
         */
        return executorManagerAdapter.getRunningFlows().size();
      }

      @Override
      public long getNumberOfCurrentSessions() {
        // Metric for flows that have been submitted, but haven't started for more than N minutes
        // (N is configurable by MIN_AGE_FOR_CLASSIFYING_A_FLOW_AGED_MINUTES).
        return sessionCache.getSessionCount();
      }

      @Override
      public long getNumberOfAgedQueuedFlows() {
        return executorManagerAdapter.getAgedQueuedFlowSize();
      }
    });

    this.webMetrics.startReporting(this.props);
  }

  /**
   * Set up and start reporting container metrics
   */
  private void startContainerMetrics() {
    this.containerizationMetrics.setUp();
    this.containerizationMetrics.startReporting(this.props);
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

  public WebMetrics getWebMetrics() {
    return this.webMetrics;
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

  @Override
  public MBeanRegistrationManager getMBeanRegistrationManager() {
    return this.mbeanRegistrationManager;
  }

  @Override
  public void configureMBeanServer() {
    logger.info("Registering MBeans...");

    this.mbeanRegistrationManager.registerMBean("jetty", new JmxJettyServer(this.server));
    this.mbeanRegistrationManager
        .registerMBean("triggerManager", new JmxTriggerManager(this.triggerManager));

    if (this.executorManagerAdapter instanceof ExecutorManager) {
      this.mbeanRegistrationManager.registerMBean("executorManager",
          new JmxExecutorManager((ExecutorManager) this.executorManagerAdapter));
    } else if (this.executorManagerAdapter instanceof ExecutionController) {
      this.mbeanRegistrationManager.registerMBean("executionController",
          new JmxExecutionController((ExecutionController) this.executorManagerAdapter));
    } else if (this.executorManagerAdapter instanceof ContainerizedDispatchManager) {
      this.mbeanRegistrationManager.registerMBean("containerizedExecutionManager",
          new JmxContainerizedDispatchManager(
              (ContainerizedDispatchManager) this.executorManagerAdapter));
    }

    // Register Log4J loggers as JMX beans so the log level can be
    // updated via JConsole or Java VisualVM
    final HierarchyDynamicMBean log4jMBean = new HierarchyDynamicMBean();
    this.mbeanRegistrationManager.registerMBean("log4jmxbean", log4jMBean);

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
    this.mbeanRegistrationManager.closeMBeans();
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

  public ObjectMapper getObjectMapper() {
    return this.objectMapper;
  }

  public ImageTypeService getImageTypeService() {
    return SERVICE_PROVIDER.getInstance(ImageTypeService.class);
  }

  public ImageVersionService getImageVersionsService() {
    return SERVICE_PROVIDER.getInstance(ImageVersionService.class);
  }

  public ImageRampupService getImageRampupService() {
    return SERVICE_PROVIDER.getInstance(ImageRampupService.class);
  }

  public PermissionManager getPermissionManager() {
    return SERVICE_PROVIDER.getInstance(PermissionManager.class);
  }

  public ConverterUtils getConverterUtils() {
    return SERVICE_PROVIDER.getInstance(ConverterUtils.class);
  }

  public ImageMgmtCommonService getImageMgmtCommonService() {
    return SERVICE_PROVIDER.getInstance(ImageMgmtCommonService.class);
  }
}

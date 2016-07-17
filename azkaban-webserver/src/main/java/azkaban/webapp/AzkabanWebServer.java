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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.jmx.HierarchyDynamicMBean;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.runtime.resource.loader.JarResourceLoader;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import azkaban.alert.Alerter;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.executor.ExecutorManager;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.jmx.JmxExecutorManager;
import azkaban.jmx.JmxJettyServer;
import azkaban.jmx.JmxTriggerManager;
import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectManager;
import azkaban.scheduler.ScheduleLoader;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.TriggerBasedScheduleLoader;
import azkaban.server.AzkabanServer;
import azkaban.server.ServerConstants;
import azkaban.server.session.SessionCache;
import azkaban.trigger.JdbcTriggerLoader;
import azkaban.trigger.TriggerLoader;
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
import azkaban.user.XmlUserManager;
import azkaban.utils.Emailer;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import azkaban.webapp.plugin.PluginRegistry;
import azkaban.webapp.plugin.TriggerPlugin;
import azkaban.webapp.plugin.ViewerPlugin;
import azkaban.webapp.servlet.AbstractAzkabanServlet;
import azkaban.webapp.servlet.ExecutorServlet;
import azkaban.webapp.servlet.HistoryServlet;
import azkaban.webapp.servlet.IndexRedirectServlet;
import azkaban.webapp.servlet.JMXHttpServlet;
import azkaban.webapp.servlet.ProjectManagerServlet;
import azkaban.webapp.servlet.ProjectServlet;
import azkaban.webapp.servlet.ScheduleServlet;
import azkaban.webapp.servlet.StatsServlet;
import azkaban.webapp.servlet.TriggerManagerServlet;

import com.linkedin.restli.server.RestliServlet;

/**
 * The Azkaban Jetty server class
 *
 * Global azkaban properties for setup. All of them are optional unless
 * otherwise marked: azkaban.name - The displayed name of this instance.
 * azkaban.label - Short descriptor of this Azkaban instance. azkaban.color -
 * Theme color azkaban.temp.dir - Temp dir used by Azkaban for various file
 * uses. web.resource.dir - The directory that contains the static web files.
 * default.timezone.id - The timezone code. I.E. America/Los Angeles
 *
 * user.manager.class - The UserManager class used for the user manager. Default
 * is XmlUserManager. project.manager.class - The ProjectManager to load
 * projects project.global.properties - The base properties inherited by all
 * projects and jobs
 *
 * jetty.maxThreads - # of threads for jetty jetty.ssl.port - The ssl port used
 * for sessionizing. jetty.keystore - Jetty keystore . jetty.keypassword - Jetty
 * keystore password jetty.truststore - Jetty truststore jetty.trustpassword -
 * Jetty truststore password
 */
public class AzkabanWebServer extends AzkabanServer {
  private static final String AZKABAN_ACCESS_LOGGER_NAME =
      "azkaban.webapp.servlet.LoginAbstractAzkabanServlet";

  private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);

  public static final String AZKABAN_HOME = "AZKABAN_HOME";
  public static final String DEFAULT_CONF_PATH = "conf";
  public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
  public static final String AZKABAN_PRIVATE_PROPERTIES_FILE =
      "azkaban.private.properties";

  private static final int MAX_FORM_CONTENT_SIZE = 10 * 1024 * 1024;
  private static final int MAX_HEADER_BUFFER_SIZE = 10 * 1024 * 1024;
  private static AzkabanWebServer app;

  private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";
  private static final int DEFAULT_PORT_NUMBER = 8081;
  private static final int DEFAULT_SSL_PORT_NUMBER = 8443;
  private static final int DEFAULT_THREAD_NUMBER = 20;
  private static final String VELOCITY_DEV_MODE_PARAM = "velocity.dev.mode";
  private static final String USER_MANAGER_CLASS_PARAM = "user.manager.class";
  private static final String DEFAULT_STATIC_DIR = "";

  private final VelocityEngine velocityEngine;

  private final Server server;
  private UserManager userManager;
  private ProjectManager projectManager;
  // private ExecutorManagerAdapter executorManager;
  private ExecutorManager executorManager;
  private ScheduleManager scheduleManager;
  private TriggerManager triggerManager;
  private Map<String, Alerter> alerters;

  private final ClassLoader baseClassLoader;

  private Props props;
  private SessionCache sessionCache;
  private File tempDir;
  private Map<String, TriggerPlugin> triggerPlugins;

  private MBeanServer mbeanServer;
  private ArrayList<ObjectName> registeredMBeans = new ArrayList<ObjectName>();

  public static AzkabanWebServer getInstance() {
    return app;
  }

  /**
   * Constructor usually called by tomcat AzkabanServletContext to create the
   * initial server
   */
  public AzkabanWebServer() throws Exception {
    this(null, loadConfigurationFromAzkabanHome());
  }

  /**
   * Constructor
   */
  public AzkabanWebServer(Server server, Props props) throws Exception {
    this.props = props;
    this.server = server;
    velocityEngine =
        configureVelocityEngine(props
            .getBoolean(VELOCITY_DEV_MODE_PARAM, false));
    sessionCache = new SessionCache(props);
    userManager = loadUserManager(props);

    alerters = loadAlerters(props);

    executorManager = loadExecutorManager(props);
    projectManager = loadProjectManager(props);

    triggerManager = loadTriggerManager(props);
    loadBuiltinCheckersAndActions();

    // load all trigger agents here
    scheduleManager = loadScheduleManager(triggerManager, props);

    String triggerPluginDir =
        props.getString("trigger.plugin.dir", "plugins/triggers");

    loadPluginCheckersAndActions(triggerPluginDir);

    baseClassLoader = this.getClassLoader();

    tempDir = new File(props.getString("azkaban.temp.dir", "temp"));

    // Setup time zone
    if (props.containsKey(DEFAULT_TIMEZONE_ID)) {
      String timezone = props.getString(DEFAULT_TIMEZONE_ID);
      System.setProperty("user.timezone", timezone);
      TimeZone.setDefault(TimeZone.getTimeZone(timezone));
      DateTimeZone.setDefault(DateTimeZone.forID(timezone));
      logger.info("Setting timezone to " + timezone);
    }

    configureMBeanServer();
  }

  private void setTriggerPlugins(Map<String, TriggerPlugin> triggerPlugins) {
    this.triggerPlugins = triggerPlugins;
  }

  private UserManager loadUserManager(Props props) {
    Class<?> userManagerClass = props.getClass(USER_MANAGER_CLASS_PARAM, null);
    logger.info("Loading user manager class " + userManagerClass.getName());
    UserManager manager = null;
    if (userManagerClass != null
        && userManagerClass.getConstructors().length > 0) {
      try {
        Constructor<?> userManagerConstructor =
            userManagerClass.getConstructor(Props.class);
        manager = (UserManager) userManagerConstructor.newInstance(props);
      } catch (Exception e) {
        logger.error("Could not instantiate UserManager "
            + userManagerClass.getName());
        throw new RuntimeException(e);
      }
    } else {
      manager = new XmlUserManager(props);
    }
    return manager;
  }

  private ProjectManager loadProjectManager(Props props) {
    logger.info("Loading JDBC for project management");
    JdbcProjectLoader loader = new JdbcProjectLoader(props);
    ProjectManager manager = new ProjectManager(loader, props);
    return manager;
  }

  private ExecutorManager loadExecutorManager(Props props) throws Exception {
    JdbcExecutorLoader loader = new JdbcExecutorLoader(props);
    ExecutorManager execManager = new ExecutorManager(props, loader, alerters);
    return execManager;
  }

  private ScheduleManager loadScheduleManager(TriggerManager tm, Props props)
      throws Exception {
    logger.info("Loading trigger based scheduler");
    ScheduleLoader loader =
        new TriggerBasedScheduleLoader(tm, ScheduleManager.triggerSource);
    return new ScheduleManager(loader);
  }

  private TriggerManager loadTriggerManager(Props props)
      throws TriggerManagerException {
    TriggerLoader loader = new JdbcTriggerLoader(props);
    return new TriggerManager(props, loader, executorManager);
  }

  private void loadBuiltinCheckersAndActions() {
    logger.info("Loading built-in checker and action types");
    if (triggerManager instanceof TriggerManager) {
      SlaChecker.setExecutorManager(executorManager);
      ExecuteFlowAction.setExecutorManager(executorManager);
      ExecuteFlowAction.setProjectManager(projectManager);
      ExecuteFlowAction.setTriggerManager(triggerManager);
      KillExecutionAction.setExecutorManager(executorManager);
      SlaAlertAction.setExecutorManager(executorManager);
      SlaAlertAction.setAlerters(alerters);
      SlaAlertAction.setExecutorManager(executorManager);
      CreateTriggerAction.setTriggerManager(triggerManager);
      ExecutionChecker.setExecutorManager(executorManager);
    }
    triggerManager.registerCheckerType(BasicTimeChecker.type,
        BasicTimeChecker.class);
    triggerManager.registerCheckerType(SlaChecker.type, SlaChecker.class);
    triggerManager.registerCheckerType(ExecutionChecker.type,
        ExecutionChecker.class);
    triggerManager.registerActionType(ExecuteFlowAction.type,
        ExecuteFlowAction.class);
    triggerManager.registerActionType(KillExecutionAction.type,
        KillExecutionAction.class);
    triggerManager
        .registerActionType(SlaAlertAction.type, SlaAlertAction.class);
    triggerManager.registerActionType(CreateTriggerAction.type,
        CreateTriggerAction.class);
  }

  private Map<String, Alerter> loadAlerters(Props props) {
    Map<String, Alerter> allAlerters = new HashMap<String, Alerter>();
    // load built-in alerters
    Emailer mailAlerter = new Emailer(props);
    allAlerters.put("email", mailAlerter);
    // load all plugin alerters
    String pluginDir = props.getString("alerter.plugin.dir", "plugins/alerter");
    allAlerters.putAll(loadPluginAlerters(pluginDir));
    return allAlerters;
  }

  private Map<String, Alerter> loadPluginAlerters(String pluginPath) {
    File alerterPluginPath = new File(pluginPath);
    if (!alerterPluginPath.exists()) {
      return Collections.<String, Alerter> emptyMap();
    }

    Map<String, Alerter> installedAlerterPlugins =
        new HashMap<String, Alerter>();
    ClassLoader parentLoader = getClass().getClassLoader();
    File[] pluginDirs = alerterPluginPath.listFiles();
    ArrayList<String> jarPaths = new ArrayList<String>();
    for (File pluginDir : pluginDirs) {
      if (!pluginDir.isDirectory()) {
        logger.error("The plugin path " + pluginDir + " is not a directory.");
        continue;
      }

      // Load the conf directory
      File propertiesDir = new File(pluginDir, "conf");
      Props pluginProps = null;
      if (propertiesDir.exists() && propertiesDir.isDirectory()) {
        File propertiesFile = new File(propertiesDir, "plugin.properties");
        File propertiesOverrideFile =
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

      String pluginName = pluginProps.getString("alerter.name");
      List<String> extLibClasspath =
          pluginProps.getStringList("alerter.external.classpaths",
              (List<String>) null);

      String pluginClass = pluginProps.getString("alerter.class");
      if (pluginClass == null) {
        logger.error("Alerter class is not set.");
      } else {
        logger.info("Plugin class " + pluginClass);
      }

      URLClassLoader urlClassLoader = null;
      File libDir = new File(pluginDir, "lib");
      if (libDir.exists() && libDir.isDirectory()) {
        File[] files = libDir.listFiles();

        ArrayList<URL> urls = new ArrayList<URL>();
        for (int i = 0; i < files.length; ++i) {
          try {
            URL url = files[i].toURI().toURL();
            urls.add(url);
          } catch (MalformedURLException e) {
            logger.error(e);
          }
        }
        if (extLibClasspath != null) {
          for (String extLib : extLibClasspath) {
            try {
              File file = new File(pluginDir, extLib);
              URL url = file.toURI().toURL();
              urls.add(url);
            } catch (MalformedURLException e) {
              logger.error(e);
            }
          }
        }

        urlClassLoader =
            new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
      } else {
        logger.error("Library path " + propertiesDir + " not found.");
        continue;
      }

      Class<?> alerterClass = null;
      try {
        alerterClass = urlClassLoader.loadClass(pluginClass);
      } catch (ClassNotFoundException e) {
        logger.error("Class " + pluginClass + " not found.");
        continue;
      }

      String source = FileIOUtils.getSourcePathFromClass(alerterClass);
      logger.info("Source jar " + source);
      jarPaths.add("jar:file:" + source);

      Constructor<?> constructor = null;
      try {
        constructor = alerterClass.getConstructor(Props.class);
      } catch (NoSuchMethodException e) {
        logger.error("Constructor not found in " + pluginClass);
        continue;
      }

      Object obj = null;
      try {
        obj = constructor.newInstance(pluginProps);
      } catch (Exception e) {
        logger.error(e);
      }

      if (!(obj instanceof Alerter)) {
        logger.error("The object is not an Alerter");
        continue;
      }

      Alerter plugin = (Alerter) obj;
      installedAlerterPlugins.put(pluginName, plugin);
    }

    return installedAlerterPlugins;

  }

  private void loadPluginCheckersAndActions(String pluginPath) {
    logger.info("Loading plug-in checker and action types");
    File triggerPluginPath = new File(pluginPath);
    if (!triggerPluginPath.exists()) {
      logger.error("plugin path " + pluginPath + " doesn't exist!");
      return;
    }

    ClassLoader parentLoader = this.getClassLoader();
    File[] pluginDirs = triggerPluginPath.listFiles();
    ArrayList<String> jarPaths = new ArrayList<String>();
    for (File pluginDir : pluginDirs) {
      if (!pluginDir.exists()) {
        logger.error("Error! Trigger plugin path " + pluginDir.getPath()
            + " doesn't exist.");
        continue;
      }

      if (!pluginDir.isDirectory()) {
        logger.error("The plugin path " + pluginDir + " is not a directory.");
        continue;
      }

      // Load the conf directory
      File propertiesDir = new File(pluginDir, "conf");
      Props pluginProps = null;
      if (propertiesDir.exists() && propertiesDir.isDirectory()) {
        File propertiesFile = new File(propertiesDir, "plugin.properties");
        File propertiesOverrideFile =
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

      List<String> extLibClasspath =
          pluginProps.getStringList("trigger.external.classpaths",
              (List<String>) null);

      String pluginClass = pluginProps.getString("trigger.class");
      if (pluginClass == null) {
        logger.error("Trigger class is not set.");
      } else {
        logger.error("Plugin class " + pluginClass);
      }

      URLClassLoader urlClassLoader = null;
      File libDir = new File(pluginDir, "lib");
      if (libDir.exists() && libDir.isDirectory()) {
        File[] files = libDir.listFiles();

        ArrayList<URL> urls = new ArrayList<URL>();
        for (int i = 0; i < files.length; ++i) {
          try {
            URL url = files[i].toURI().toURL();
            urls.add(url);
          } catch (MalformedURLException e) {
            logger.error(e);
          }
        }
        if (extLibClasspath != null) {
          for (String extLib : extLibClasspath) {
            try {
              File file = new File(pluginDir, extLib);
              URL url = file.toURI().toURL();
              urls.add(url);
            } catch (MalformedURLException e) {
              logger.error(e);
            }
          }
        }

        urlClassLoader =
            new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
      } else {
        logger.error("Library path " + propertiesDir + " not found.");
        continue;
      }

      Class<?> triggerClass = null;
      try {
        triggerClass = urlClassLoader.loadClass(pluginClass);
      } catch (ClassNotFoundException e) {
        logger.error("Class " + pluginClass + " not found.");
        continue;
      }

      String source = FileIOUtils.getSourcePathFromClass(triggerClass);
      logger.info("Source jar " + source);
      jarPaths.add("jar:file:" + source);

      try {
        Utils.invokeStaticMethod(urlClassLoader, pluginClass,
            "initiateCheckerTypes", pluginProps, app);
      } catch (Exception e) {
        logger.error("Unable to initiate checker types for " + pluginClass);
        continue;
      }

      try {
        Utils.invokeStaticMethod(urlClassLoader, pluginClass,
            "initiateActionTypes", pluginProps, app);
      } catch (Exception e) {
        logger.error("Unable to initiate action types for " + pluginClass);
        continue;
      }

    }
  }

  /**
   * Returns the web session cache.
   *
   * @return
   */
  public SessionCache getSessionCache() {
    return sessionCache;
  }

  /**
   * Returns the velocity engine for pages to use.
   *
   * @return
   */
  public VelocityEngine getVelocityEngine() {
    return velocityEngine;
  }

  /**
   *
   * @return
   */
  public UserManager getUserManager() {
    return userManager;
  }

  /**
   *
   * @return
   */
  public ProjectManager getProjectManager() {
    return projectManager;
  }

  /**
   *
   */
  public ExecutorManager getExecutorManager() {
    return executorManager;
  }

  public ScheduleManager getScheduleManager() {
    return scheduleManager;
  }

  public TriggerManager getTriggerManager() {
    return triggerManager;
  }

  /**
   * Creates and configures the velocity engine.
   *
   * @param devMode
   * @return
   */
  private VelocityEngine configureVelocityEngine(final boolean devMode) {
    VelocityEngine engine = new VelocityEngine();
    engine.setProperty("resource.loader", "classpath, jar");
    engine.setProperty("classpath.resource.loader.class",
        ClasspathResourceLoader.class.getName());
    engine.setProperty("classpath.resource.loader.cache", !devMode);
    engine.setProperty("classpath.resource.loader.modificationCheckInterval",
        5L);
    engine.setProperty("jar.resource.loader.class",
        JarResourceLoader.class.getName());
    engine.setProperty("jar.resource.loader.cache", !devMode);
    engine.setProperty("resource.manager.logwhenfound", false);
    engine.setProperty("input.encoding", "UTF-8");
    engine.setProperty("output.encoding", "UTF-8");
    engine.setProperty("directive.set.null.allowed", true);
    engine.setProperty("resource.manager.logwhenfound", false);
    engine.setProperty("velocimacro.permissions.allow.inline", true);
    engine.setProperty("velocimacro.library.autoreload", devMode);
    engine.setProperty("velocimacro.library",
        "/azkaban/webapp/servlet/velocity/macros.vm");
    engine.setProperty(
        "velocimacro.permissions.allow.inline.to.replace.global", true);
    engine.setProperty("velocimacro.arguments.strict", true);
    engine.setProperty("runtime.log.invalid.references", devMode);
    engine.setProperty("runtime.log.logsystem.class", Log4JLogChute.class);
    engine.setProperty("runtime.log.logsystem.log4j.logger",
        Logger.getLogger("org.apache.velocity.Logger"));
    engine.setProperty("parser.pool.size", 3);
    return engine;
  }

  public ClassLoader getClassLoader() {
    return baseClassLoader;
  }

  /**
   * Returns the global azkaban properties
   *
   * @return
   */
  public Props getServerProps() {
    return props;
  }

  /**
   * Azkaban using Jetty
   *
   * @param args
   */
  public static void main(String[] args) throws Exception {
    logger.info("Starting Jetty Azkaban Web Server...");
    Props azkabanSettings = AzkabanServer.loadProps(args);

    if (azkabanSettings == null) {
      logger.error("Azkaban Properties not loaded.");
      logger.error("Exiting Azkaban...");
      return;
    }

    int maxThreads =
        azkabanSettings.getInt("jetty.maxThreads", DEFAULT_THREAD_NUMBER);
    boolean isStatsOn =
        azkabanSettings.getBoolean("jetty.connector.stats", true);
    logger.info("Setting up connector with stats on: " + isStatsOn);

    boolean ssl;
    int port;
    final Server server = new Server();
    if (azkabanSettings.getBoolean("jetty.use.ssl", true)) {
      int sslPortNumber =
          azkabanSettings.getInt("jetty.ssl.port", DEFAULT_SSL_PORT_NUMBER);
      port = sslPortNumber;
      ssl = true;
      logger.info("Setting up Jetty Https Server with port:" + sslPortNumber
          + " and numThreads:" + maxThreads);

      SslSocketConnector secureConnector = new SslSocketConnector();
      secureConnector.setPort(sslPortNumber);
      secureConnector.setKeystore(azkabanSettings.getString("jetty.keystore"));
      secureConnector.setPassword(azkabanSettings.getString("jetty.password"));
      secureConnector.setKeyPassword(azkabanSettings
          .getString("jetty.keypassword"));
      secureConnector.setTruststore(azkabanSettings
          .getString("jetty.truststore"));
      secureConnector.setTrustPassword(azkabanSettings
          .getString("jetty.trustpassword"));
      secureConnector.setHeaderBufferSize(MAX_HEADER_BUFFER_SIZE);

      // set up vulnerable cipher suites to exclude
      List<String> cipherSuitesToExclude = azkabanSettings.getStringList("jetty.excludeCipherSuites");
      logger.info("Excluded Cipher Suites: " + String.valueOf(cipherSuitesToExclude));
      if (cipherSuitesToExclude != null && !cipherSuitesToExclude.isEmpty()) {
        secureConnector.setExcludeCipherSuites(cipherSuitesToExclude.toArray(new String[0]));
      }

      server.addConnector(secureConnector);
    } else {
      ssl = false;
      port = azkabanSettings.getInt("jetty.port", DEFAULT_PORT_NUMBER);
      SocketConnector connector = new SocketConnector();
      connector.setPort(port);
      connector.setHeaderBufferSize(MAX_HEADER_BUFFER_SIZE);
      server.addConnector(connector);
    }

    // setting stats configuration for connectors
    for (Connector connector : server.getConnectors()) {
      connector.setStatsOn(isStatsOn);
    }

    String hostname = azkabanSettings.getString("jetty.hostname", "localhost");
    azkabanSettings.put("server.hostname", hostname);
    azkabanSettings.put("server.port", port);
    azkabanSettings.put("server.useSSL", String.valueOf(ssl));

    app = new AzkabanWebServer(server, azkabanSettings);

    boolean checkDB =
        azkabanSettings.getBoolean(AzkabanDatabaseSetup.DATABASE_CHECK_VERSION,
            false);
    if (checkDB) {
      AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(azkabanSettings);
      setup.loadTableInfo();
      if (setup.needsUpdating()) {
        logger.error("Database is out of date.");
        setup.printUpgradePlan();

        logger.error("Exiting with error.");
        System.exit(-1);
      }
    }

    QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
    server.setThreadPool(httpThreadPool);

    String staticDir =
        azkabanSettings.getString("web.resource.dir", DEFAULT_STATIC_DIR);
    logger.info("Setting up web resource dir " + staticDir);
    Context root = new Context(server, "/", Context.SESSIONS);
    root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);

    String defaultServletPath =
        azkabanSettings.getString("azkaban.default.servlet.path", "/index");
    root.setResourceBase(staticDir);
    ServletHolder indexRedirect =
        new ServletHolder(new IndexRedirectServlet(defaultServletPath));
    root.addServlet(indexRedirect, "/");
    ServletHolder index = new ServletHolder(new ProjectServlet());
    root.addServlet(index, "/index");

    ServletHolder staticServlet = new ServletHolder(new DefaultServlet());
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

    ServletHolder restliHolder = new ServletHolder(new RestliServlet());
    restliHolder.setInitParameter("resourcePackages", "azkaban.restli");
    root.addServlet(restliHolder, "/restli/*");

    String viewerPluginDir =
        azkabanSettings.getString("viewer.plugin.dir", "plugins/viewer");
    loadViewerPlugins(root, viewerPluginDir, app.getVelocityEngine());

    // triggerplugin
    String triggerPluginDir =
        azkabanSettings.getString("trigger.plugin.dir", "plugins/triggers");
    Map<String, TriggerPlugin> triggerPlugins =
        loadTriggerPlugins(root, triggerPluginDir, app);
    app.setTriggerPlugins(triggerPlugins);
    // always have basic time trigger
    // TODO: find something else to do the job
    app.getTriggerManager().start();

    root.setAttribute(ServerConstants.AZKABAN_SERVLET_CONTEXT_KEY, app);
    try {
      server.start();
    } catch (Exception e) {
      logger.warn(e);
      Utils.croak(e.getMessage(), 1);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {

      public void run() {
        try {
          logTopMemoryConsumers();
        } catch (Exception e) {
          logger.info(("Exception when logging top memory consumers"), e);
        }

        logger.info("Shutting down http server...");
        try {
          app.close();
          server.stop();
          server.destroy();
        } catch (Exception e) {
          logger.error("Error while shutting down http server.", e);
        }
        logger.info("kk thx bye.");
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
    logger.info("Server running on " + (ssl ? "ssl" : "") + " port " + port
        + ".");
  }

  private static Map<String, TriggerPlugin> loadTriggerPlugins(Context root,
      String pluginPath, AzkabanWebServer azkabanWebApp) {
    File triggerPluginPath = new File(pluginPath);
    if (!triggerPluginPath.exists()) {
      return new HashMap<String, TriggerPlugin>();
    }

    Map<String, TriggerPlugin> installedTriggerPlugins =
        new HashMap<String, TriggerPlugin>();
    ClassLoader parentLoader = AzkabanWebServer.class.getClassLoader();
    File[] pluginDirs = triggerPluginPath.listFiles();
    ArrayList<String> jarPaths = new ArrayList<String>();
    for (File pluginDir : pluginDirs) {
      if (!pluginDir.exists()) {
        logger.error("Error! Trigger plugin path " + pluginDir.getPath()
            + " doesn't exist.");
        continue;
      }

      if (!pluginDir.isDirectory()) {
        logger.error("The plugin path " + pluginDir + " is not a directory.");
        continue;
      }

      // Load the conf directory
      File propertiesDir = new File(pluginDir, "conf");
      Props pluginProps = null;
      if (propertiesDir.exists() && propertiesDir.isDirectory()) {
        File propertiesFile = new File(propertiesDir, "plugin.properties");
        File propertiesOverrideFile =
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

      String pluginName = pluginProps.getString("trigger.name");
      List<String> extLibClasspath =
          pluginProps.getStringList("trigger.external.classpaths",
              (List<String>) null);

      String pluginClass = pluginProps.getString("trigger.class");
      if (pluginClass == null) {
        logger.error("Trigger class is not set.");
      } else {
        logger.error("Plugin class " + pluginClass);
      }

      URLClassLoader urlClassLoader = null;
      File libDir = new File(pluginDir, "lib");
      if (libDir.exists() && libDir.isDirectory()) {
        File[] files = libDir.listFiles();

        ArrayList<URL> urls = new ArrayList<URL>();
        for (int i = 0; i < files.length; ++i) {
          try {
            URL url = files[i].toURI().toURL();
            urls.add(url);
          } catch (MalformedURLException e) {
            logger.error(e);
          }
        }
        if (extLibClasspath != null) {
          for (String extLib : extLibClasspath) {
            try {
              File file = new File(pluginDir, extLib);
              URL url = file.toURI().toURL();
              urls.add(url);
            } catch (MalformedURLException e) {
              logger.error(e);
            }
          }
        }

        urlClassLoader =
            new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
      } else {
        logger.error("Library path " + propertiesDir + " not found.");
        continue;
      }

      Class<?> triggerClass = null;
      try {
        triggerClass = urlClassLoader.loadClass(pluginClass);
      } catch (ClassNotFoundException e) {
        logger.error("Class " + pluginClass + " not found.");
        continue;
      }

      String source = FileIOUtils.getSourcePathFromClass(triggerClass);
      logger.info("Source jar " + source);
      jarPaths.add("jar:file:" + source);

      Constructor<?> constructor = null;
      try {
        constructor =
            triggerClass.getConstructor(String.class, Props.class,
                Context.class, AzkabanWebServer.class);
      } catch (NoSuchMethodException e) {
        logger.error("Constructor not found in " + pluginClass);
        continue;
      }

      Object obj = null;
      try {
        obj =
            constructor.newInstance(pluginName, pluginProps, root,
                azkabanWebApp);
      } catch (Exception e) {
        logger.error(e);
      }

      if (!(obj instanceof TriggerPlugin)) {
        logger.error("The object is not an TriggerPlugin");
        continue;
      }

      TriggerPlugin plugin = (TriggerPlugin) obj;
      installedTriggerPlugins.put(pluginName, plugin);
    }

    // Velocity needs the jar resource paths to be set.
    String jarResourcePath = StringUtils.join(jarPaths, ", ");
    logger.info("Setting jar resource path " + jarResourcePath);
    VelocityEngine ve = azkabanWebApp.getVelocityEngine();
    ve.addProperty("jar.resource.loader.path", jarResourcePath);

    return installedTriggerPlugins;
  }

  public Map<String, TriggerPlugin> getTriggerPlugins() {
    return triggerPlugins;
  }

  private static void loadViewerPlugins(Context root, String pluginPath,
      VelocityEngine ve) {
    File viewerPluginPath = new File(pluginPath);
    if (!viewerPluginPath.exists()) {
      return;
    }

    ClassLoader parentLoader = AzkabanWebServer.class.getClassLoader();
    File[] pluginDirs = viewerPluginPath.listFiles();
    ArrayList<String> jarPaths = new ArrayList<String>();
    for (File pluginDir : pluginDirs) {
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
      File propertiesDir = new File(pluginDir, "conf");
      Props pluginProps = null;
      if (propertiesDir.exists() && propertiesDir.isDirectory()) {
        File propertiesFile = new File(propertiesDir, "plugin.properties");
        File propertiesOverrideFile =
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

      String pluginName = pluginProps.getString("viewer.name");
      String pluginWebPath = pluginProps.getString("viewer.path");
      String pluginJobTypes = pluginProps.getString("viewer.jobtypes", null);
      int pluginOrder = pluginProps.getInt("viewer.order", 0);
      boolean pluginHidden = pluginProps.getBoolean("viewer.hidden", false);
      List<String> extLibClasspath =
          pluginProps.getStringList("viewer.external.classpaths",
              (List<String>) null);

      String pluginClass = pluginProps.getString("viewer.servlet.class");
      if (pluginClass == null) {
        logger.error("Viewer class is not set.");
      } else {
        logger.info("Plugin class " + pluginClass);
      }

      URLClassLoader urlClassLoader = null;
      File libDir = new File(pluginDir, "lib");
      if (libDir.exists() && libDir.isDirectory()) {
        File[] files = libDir.listFiles();

        ArrayList<URL> urls = new ArrayList<URL>();
        for (int i = 0; i < files.length; ++i) {
          try {
            URL url = files[i].toURI().toURL();
            urls.add(url);
          } catch (MalformedURLException e) {
            logger.error(e);
          }
        }

        // Load any external libraries.
        if (extLibClasspath != null) {
          for (String extLib : extLibClasspath) {
            File extLibFile = new File(pluginDir, extLib);
            if (extLibFile.exists()) {
              if (extLibFile.isDirectory()) {
                // extLibFile is a directory; load all the files in the
                // directory.
                File[] extLibFiles = extLibFile.listFiles();
                for (int i = 0; i < extLibFiles.length; ++i) {
                  try {
                    URL url = extLibFiles[i].toURI().toURL();
                    urls.add(url);
                  } catch (MalformedURLException e) {
                    logger.error(e);
                  }
                }
              } else { // extLibFile is a file
                try {
                  URL url = extLibFile.toURI().toURL();
                  urls.add(url);
                } catch (MalformedURLException e) {
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
      } catch (ClassNotFoundException e) {
        logger.error("Class " + pluginClass + " not found.");
        continue;
      }

      String source = FileIOUtils.getSourcePathFromClass(viewerClass);
      logger.info("Source jar " + source);
      jarPaths.add("jar:file:" + source);

      Constructor<?> constructor = null;
      try {
        constructor = viewerClass.getConstructor(Props.class);
      } catch (NoSuchMethodException e) {
        logger.error("Constructor not found in " + pluginClass);
        continue;
      }

      Object obj = null;
      try {
        obj = constructor.newInstance(pluginProps);
      } catch (Exception e) {
        logger.error(e);
        logger.error(e.getCause());
      }

      if (!(obj instanceof AbstractAzkabanServlet)) {
        logger.error("The object is not an AbstractAzkabanServlet");
        continue;
      }

      AbstractAzkabanServlet avServlet = (AbstractAzkabanServlet) obj;
      root.addServlet(new ServletHolder(avServlet), "/" + pluginWebPath + "/*");
      PluginRegistry.getRegistry().register(
          new ViewerPlugin(pluginName, pluginWebPath, pluginOrder,
              pluginHidden, pluginJobTypes));
    }

    // Velocity needs the jar resource paths to be set.
    String jarResourcePath = StringUtils.join(jarPaths, ", ");
    logger.info("Setting jar resource path " + jarResourcePath);
    ve.addProperty("jar.resource.loader.path", jarResourcePath);
  }

  /**
   * Loads the Azkaban property file from the AZKABAN_HOME conf directory
   *
   * @return
   */
  private static Props loadConfigurationFromAzkabanHome() {
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

    File confPath = new File(azkabanHome, DEFAULT_CONF_PATH);
    if (!confPath.exists() || !confPath.isDirectory() || !confPath.canRead()) {
      logger
          .error(azkabanHome + " does not contain a readable conf directory.");
      return null;
    }

    return loadAzkabanConfigurationFromDirectory(confPath);
  }

  /**
   * Returns the set temp dir
   *
   * @return
   */
  public File getTempDirectory() {
    return tempDir;
  }

  private static Props loadAzkabanConfigurationFromDirectory(File dir) {
    File azkabanPrivatePropsFile =
        new File(dir, AZKABAN_PRIVATE_PROPERTIES_FILE);
    File azkabanPropsFile = new File(dir, AZKABAN_PROPERTIES_FILE);

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

    registerMbean("jetty", new JmxJettyServer(server));
    registerMbean("triggerManager", new JmxTriggerManager(triggerManager));
    if (executorManager instanceof ExecutorManager) {
      registerMbean("executorManager", new JmxExecutorManager(
          (ExecutorManager) executorManager));
    }

    // Register Log4J loggers as JMX beans so the log level can be
    // updated via JConsole or Java VisualVM
    HierarchyDynamicMBean log4jMBean = new HierarchyDynamicMBean();
    registerMbean("log4jmxbean", log4jMBean);
    ObjectName accessLogLoggerObjName =
        log4jMBean.addLoggerMBean(AZKABAN_ACCESS_LOGGER_NAME);

    if (accessLogLoggerObjName == null) {
      System.out
          .println("************* loginLoggerObjName is null, make sure there is a logger with name "
              + AZKABAN_ACCESS_LOGGER_NAME);
    } else {
      System.out.println("******** loginLoggerObjName: "
          + accessLogLoggerObjName.getCanonicalName());
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
    scheduleManager.shutdown();
    executorManager.shutdown();
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
}

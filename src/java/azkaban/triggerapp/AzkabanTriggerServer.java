package azkaban.triggerapp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import azkaban.executor.ExecutorMailer;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerRemoteAdapter;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.executor.ExecutorManager.Alerter;
import azkaban.jmx.JmxExecutorManagerAdapter;
import azkaban.jmx.JmxJettyServer;
import azkaban.jmx.JmxTriggerManager;
import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectManager;
import azkaban.trigger.JdbcTriggerLoader;
import azkaban.trigger.TriggerLoader;
import azkaban.trigger.TriggerManagerException;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerAdapter;
import azkaban.trigger.TriggerManagerServlet;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.CreateTriggerAction;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.trigger.builtin.KillExecutionAction;
import azkaban.trigger.builtin.SlaAlertAction;
import azkaban.trigger.builtin.SlaChecker;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanServer;
import azkaban.webapp.servlet.AzkabanServletContextListener;

public class AzkabanTriggerServer {
	private static final Logger logger = Logger.getLogger(AzkabanTriggerServer.class);
	private static final int MAX_FORM_CONTENT_SIZE = 10*1024*1024;

	public static final String AZKABAN_HOME = "AZKABAN_HOME";
	public static final String DEFAULT_CONF_PATH = "conf";
	public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
	public static final String AZKABAN_PRIVATE_PROPERTIES_FILE = "azkaban.private.properties";
	public static final String TRIGGER_PLUGIN_DIR = "trigger.plugin.dir";
	public static final int DEFAULT_PORT_NUMBER = 22321;
	public static final int DEFAULT_THREAD_NUMBER = 50;
	
	private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";

	private static AzkabanTriggerServer app;
	
	private TriggerLoader triggerLoader;
	private TriggerManager triggerManager;
	private ExecutorManager executorManager;
	private ProjectManager projectManager;
	private Props props;
	private Server server;
	
	private ArrayList<ObjectName> registeredMBeans = new ArrayList<ObjectName>();
	private MBeanServer mbeanServer;

	/**
	 * Constructor
	 * 
	 * @throws Exception
	 */
	public AzkabanTriggerServer(Props props) throws Exception {
		this.props = props;

		int portNumber = props.getInt("trigger.server.port", DEFAULT_PORT_NUMBER);
		int maxThreads = props.getInt("trigger.server.maxThreads", DEFAULT_THREAD_NUMBER);

		String hostname = props.getString("jetty.hostname", "localhost");
		props.put("server.hostname", hostname);
		props.put("server.port", portNumber);
		props.put("server.useSSL", String.valueOf(props.getBoolean("jetty.use.ssl", true)));
		
		server = new Server(portNumber);
		QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
		server.setThreadPool(httpThreadPool);

		Context root = new Context(server, "/", Context.SESSIONS);
		root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);
		
		root.addServlet(new ServletHolder(new TriggerManagerServlet()), TriggerManagerServlet.WEB_PATH);
		root.addServlet(new ServletHolder(new JMXHttpServlet()), "/jmx");
		root.setAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY, this);
		
		triggerLoader = createTriggerLoader(props);
		projectManager = loadProjectManager(props);
		executorManager = loadExecutorManager(props);
		triggerManager = loadTriggerManager(props, triggerLoader);
		
		String triggerPluginDir = props.getString("trigger.plugin.dir", "plugins/triggers");
		loadBuiltinCheckersAndActions();
		loadPluginCheckersAndActions(triggerPluginDir);
		
		configureMBeanServer();
		
		try {
			triggerManager.start();
			server.start();
		} 
		catch (Exception e) {
			logger.warn(e);
			Utils.croak(e.getMessage(), 1);
		}
		
		logger.info("Azkaban Trigger Server started on port " + portNumber);
	}

	
	
	
	private TriggerManager loadTriggerManager(Props props, TriggerLoader triggerLoader) throws Exception {
		logger.info("Loading trigger manager");
		TriggerManager trm;
		try {
			trm = new TriggerManager(props, triggerLoader);
		} catch (TriggerManagerException e) {
			throw new Exception(e);
		}
		return trm;
	}
	
	private TriggerManagerAdapter loadTriggerRunnerManagerAdapter(Props props, TriggerLoader triggerLoader) throws Exception {
		TriggerManagerAdapter trmAdapter;
		String trmMode = props.getString("trigger.runner.manager.mode", "local");
		try {
			if(trmMode.equals("local")) {
				trmAdapter = new TriggerManager(props, triggerLoader);
			} else if(trmMode.equals("remote")) {
				trmAdapter = null;
			} else {
				throw new TriggerManagerException("Unknown trigger runner manager mode " + trmMode);
			}
		} catch(Exception e) {
			throw new Exception("Failed to load Trigger Runner Manager: " + e.getMessage());
		}
		return trmAdapter;
	}
	
	private ExecutorManager loadExecutorManager(Props props) throws Exception {
		logger.info("Loading executor manager");
		JdbcExecutorLoader loader = new JdbcExecutorLoader(props);
		ExecutorManager execManager = new ExecutorManager(props, loader);
		return execManager;
	}
	
	private ExecutorManagerAdapter loadExecutorManagerAdapter(Props props) throws Exception {
//		JdbcExecutorLoader loader = new JdbcExecutorLoader(props);
//		ExecutorManager execManager = new ExecutorManager(props, loader, true);
//		return execManager;
		String executorMode = props.getString("executor.manager.mode", "local");
		ExecutorManagerAdapter adapter;
		if(executorMode.equals("local")) {
			adapter = loadExecutorManager(props);
		} else if(executorMode.equals("remote")) {
			JdbcExecutorLoader loader = new JdbcExecutorLoader(props);
			adapter = new ExecutorManagerRemoteAdapter(props, loader);
		} else {
			throw new Exception("Unknown ExecutorManager mode " + executorMode);
		}
		return adapter;
	}
	
	private ProjectManager loadProjectManager(Props props) {
		logger.info("Loading project manager");
		JdbcProjectLoader loader = new JdbcProjectLoader(props);
		ProjectManager manager = new ProjectManager(loader, props);
		return manager;
	}
	
	private void loadBuiltinCheckersAndActions() {
		logger.info("Loading built-in checker and action types");
		
		if(triggerManager instanceof TriggerManager) {
			SlaChecker.setExecutorManager(executorManager);
			ExecuteFlowAction.setExecutorManager(executorManager);
			ExecuteFlowAction.setProjectManager(projectManager);
			ExecuteFlowAction.setTriggerManager(triggerManager);
			KillExecutionAction.setExecutorManager(executorManager);
			SlaAlertAction.setExecutorManager(executorManager);
			Map<String, Alerter> alerters = loadAlerters(props);
			SlaAlertAction.setAlerters(alerters);
			SlaAlertAction.setExecutorManager(executorManager);
			CreateTriggerAction.setTriggerManager(triggerManager);
		}

		triggerManager.registerCheckerType(BasicTimeChecker.type, BasicTimeChecker.class);
		triggerManager.registerCheckerType(SlaChecker.type, SlaChecker.class);
		triggerManager.registerActionType(ExecuteFlowAction.type, ExecuteFlowAction.class);
		triggerManager.registerActionType(KillExecutionAction.type, KillExecutionAction.class);
		triggerManager.registerActionType(SlaAlertAction.type, SlaAlertAction.class);
		triggerManager.registerActionType(CreateTriggerAction.type, CreateTriggerAction.class);
	}
	
	private void loadPluginCheckersAndActions(String pluginPath) {
		logger.info("Loading plug-in checker and action types");
		File triggerPluginPath = new File(pluginPath);
		if (!triggerPluginPath.exists()) {
			logger.error("plugin path " + pluginPath + " doesn't exist!");
			return;
		}
			
		ClassLoader parentLoader = this.getClass().getClassLoader();
		File[] pluginDirs = triggerPluginPath.listFiles();
		ArrayList<String> jarPaths = new ArrayList<String>();
		for (File pluginDir: pluginDirs) {
			if (!pluginDir.exists()) {
				logger.error("Error! Trigger plugin path " + pluginDir.getPath() + " doesn't exist.");
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
				File propertiesOverrideFile = new File(propertiesDir, "override.properties");
				
				if (propertiesFile.exists()) {
					if (propertiesOverrideFile.exists()) {
						pluginProps = PropsUtils.loadProps(null, propertiesFile, propertiesOverrideFile);
					}
					else {
						pluginProps = PropsUtils.loadProps(null, propertiesFile);
					}
				}
				else {
					logger.error("Plugin conf file " + propertiesFile + " not found.");
					continue;
				}
			}
			else {
				logger.error("Plugin conf path " + propertiesDir + " not found.");
				continue;
			}
			
			List<String> extLibClasspath = pluginProps.getStringList("trigger.external.classpaths", (List<String>)null);
			
			String pluginClass = pluginProps.getString("trigger.class");
			if (pluginClass == null) {
				logger.error("Trigger class is not set.");
			}
			else {
				logger.error("Plugin class " + pluginClass);
			}
			
			URLClassLoader urlClassLoader = null;
			File libDir = new File(pluginDir, "lib");
			if (libDir.exists() && libDir.isDirectory()) {
				File[] files = libDir.listFiles();
				
				ArrayList<URL> urls = new ArrayList<URL>();
				for (int i=0; i < files.length; ++i) {
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
				
				urlClassLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
			}
			else {
				logger.error("Library path " + propertiesDir + " not found.");
				continue;
			}
			
			Class<?> triggerClass = null;
			try {
				triggerClass = urlClassLoader.loadClass(pluginClass);
			}
			catch (ClassNotFoundException e) {
				logger.error("Class " + pluginClass + " not found.");
				continue;
			}

			String source = FileIOUtils.getSourcePathFromClass(triggerClass);
			logger.info("Source jar " + source);
			jarPaths.add("jar:file:" + source);
			
//			Constructor<?> constructor = null;
//			try {
//				constructor = triggerClass.getConstructor(String.class, Props.class, Context.class, AzkabanTriggerServer.class);
//			} catch (NoSuchMethodException e) {
//				logger.error("Constructor not found in " + pluginClass);
//				continue;
//			}
			try {
				Utils.invokeStaticMethod(urlClassLoader, pluginClass, "initiateCheckerTypes", pluginProps, app);
			} catch (Exception e) {
				logger.error("Unable to initiate checker types for " + pluginClass);
				continue;
			}
			
			try {
				Utils.invokeStaticMethod(urlClassLoader, pluginClass, "initiateActionTypes", pluginProps, app);
			} catch (Exception e) {
				logger.error("Unable to initiate action types for " + pluginClass);
				continue;
			}
			
		}
	}
	
	private Map<String, Alerter> loadAlerters(Props props) {
		Map<String, Alerter> allAlerters = new HashMap<String, Alerter>();
		// load built-in alerters
		ExecutorMailer mailAlerter = new ExecutorMailer(props);
		allAlerters.put("email", mailAlerter);
		// load all plugin alerters
		String pluginDir = props.getString("alerter.plugin.dir", "plugins/alerter");
		allAlerters.putAll(loadPluginAlerters(pluginDir));
		return allAlerters;
	}

	private Map<String, Alerter> loadPluginAlerters(String pluginPath) {
		File alerterPluginPath = new File(pluginPath);
		if (!alerterPluginPath.exists()) {
			return Collections.<String, Alerter>emptyMap();
		}
			
		Map<String, Alerter> installedAlerterPlugins = new HashMap<String, Alerter>();
		ClassLoader parentLoader = SlaAlertAction.class.getClass().getClassLoader();
		File[] pluginDirs = alerterPluginPath.listFiles();
		ArrayList<String> jarPaths = new ArrayList<String>();
		for (File pluginDir: pluginDirs) {
			if (!pluginDir.isDirectory()) {
				logger.error("The plugin path " + pluginDir + " is not a directory.");
				continue;
			}
			
			// Load the conf directory
			File propertiesDir = new File(pluginDir, "conf");
			Props pluginProps = null;
			if (propertiesDir.exists() && propertiesDir.isDirectory()) {
				File propertiesFile = new File(propertiesDir, "plugin.properties");
				File propertiesOverrideFile = new File(propertiesDir, "override.properties");
				
				if (propertiesFile.exists()) {
					if (propertiesOverrideFile.exists()) {
						pluginProps = PropsUtils.loadProps(null, propertiesFile, propertiesOverrideFile);
					}
					else {
						pluginProps = PropsUtils.loadProps(null, propertiesFile);
					}
				}
				else {
					logger.error("Plugin conf file " + propertiesFile + " not found.");
					continue;
				}
			}
			else {
				logger.error("Plugin conf path " + propertiesDir + " not found.");
				continue;
			}
			
			String pluginName = pluginProps.getString("alerter.name");
			List<String> extLibClasspath = pluginProps.getStringList("alerter.external.classpaths", (List<String>)null);
			
			String pluginClass = pluginProps.getString("alerter.class");
			if (pluginClass == null) {
				logger.error("Alerter class is not set.");
			}
			else {
				logger.info("Plugin class " + pluginClass);
			}
			
			URLClassLoader urlClassLoader = null;
			File libDir = new File(pluginDir, "lib");
			if (libDir.exists() && libDir.isDirectory()) {
				File[] files = libDir.listFiles();
				
				ArrayList<URL> urls = new ArrayList<URL>();
				for (int i=0; i < files.length; ++i) {
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
				
				urlClassLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
			}
			else {
				logger.error("Library path " + propertiesDir + " not found.");
				continue;
			}
			
			Class<?> alerterClass = null;
			try {
				alerterClass = urlClassLoader.loadClass(pluginClass);
			}
			catch (ClassNotFoundException e) {
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
	
	private TriggerLoader createTriggerLoader(Props props) {
		return new JdbcTriggerLoader(props);
	}

	public void stopServer() throws Exception {
		server.stop();
		server.destroy();
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
	 * Azkaban using Jetty
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		logger.error("Starting Jetty Azkaban Trigger Server...");
		Props azkabanSettings = AzkabanServer.loadProps(args);

		if (azkabanSettings == null) {
			logger.error("Azkaban Properties not loaded.");
			logger.error("Exiting Azkaban Trigger Server...");
			return;
		}

		// Setup time zone
		if (azkabanSettings.containsKey(DEFAULT_TIMEZONE_ID)) {
			String timezone = azkabanSettings.getString(DEFAULT_TIMEZONE_ID);
			TimeZone.setDefault(TimeZone.getTimeZone(timezone));
			DateTimeZone.setDefault(DateTimeZone.forID(timezone));

			logger.info("Setting timezone to " + timezone);
		}

		app = new AzkabanTriggerServer(azkabanSettings);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				logger.info("Shutting down http server...");
				try {
					app.stopServer();
				} catch (Exception e) {
					logger.error("Error while shutting down http server.", e);
				}
				logger.info("kk thx bye.");
			}
		});
	}

	/**
	 * Loads the Azkaban property file from the AZKABAN_HOME conf directory
	 * 
	 * @return
	 */
	/*package*/ static Props loadConfigurationFromAzkabanHome() {
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
		if (!confPath.exists() || !confPath.isDirectory()
				|| !confPath.canRead()) {
			logger.error(azkabanHome
					+ " does not contain a readable conf directory.");
			return null;
		}

		return loadAzkabanConfigurationFromDirectory(confPath);
	}

	/**
	 * Loads the Azkaban conf file int a Props object
	 * 
	 * @param path
	 * @return
	 */
	private static Props loadAzkabanConfigurationFromDirectory(File dir) {
		File azkabanPrivatePropsFile = new File(dir, AZKABAN_PRIVATE_PROPERTIES_FILE);
		File azkabanPropsFile = new File(dir, AZKABAN_PROPERTIES_FILE);
		
		Props props = null;
		try {
			// This is purely optional
			if (azkabanPrivatePropsFile.exists() && azkabanPrivatePropsFile.isFile()) {
				logger.info("Loading azkaban private properties file" );
				props = new Props(null, azkabanPrivatePropsFile);
			}

			if (azkabanPropsFile.exists() && azkabanPropsFile.isFile()) {
				logger.info("Loading azkaban properties file" );
				props = new Props(props, azkabanPropsFile);
			}
		} catch (FileNotFoundException e) {
			logger.error("File not found. Could not load azkaban config file", e);
		} catch (IOException e) {
			logger.error("File found, but error reading. Could not load azkaban config file", e);
		}
		
		return props;
	}

	private void configureMBeanServer() {
		logger.info("Registering MBeans...");
		mbeanServer = ManagementFactory.getPlatformMBeanServer();

		registerMbean("triggerServerJetty", new JmxJettyServer(server));
//		if(triggerRunnerManager instanceof TriggerRunnerManagerLocalAdapter) {
//			registerMbean("triggerRunnerManager", new JmxTriggerRunnerManager(((TriggerRunnerManagerLocalAdapter)triggerRunnerManager).getTriggerRunnerManager()));
//		}
		registerMbean("triggerManager", new JmxTriggerManager(triggerManager));
//		if(executorManager instanceof ExecutorManagerLocalAdapter) {
//			registerMbean("executorManager", new JmxExecutorManager(((ExecutorManagerLocalAdapter)executorManager).getExecutorManager()));
//		}
		registerMbean("executorManager", new JmxExecutorManagerAdapter(executorManager));
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
			logger.error("Error registering mbean " + mbeanClass.getCanonicalName(), e);
		}
//		if(executorManager instanceof ExecutorManagerLocalAdapter) {
//			((ExecutorManagerLocalAdapter)executorManager).getExecutorManager().shutdown();
//		}
//		if(triggerRunnerManager instanceof TriggerRunnerManagerLocalAdapter) {
//			
//		}
		executorManager.shutdown();
		triggerManager.shutdown();
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

	public TriggerManager getTriggerManager() {
		return triggerManager;
	}
	
	public ExecutorManager getExecutorManager() {
		return executorManager;
	}
	
	public ProjectManager getProjectManager() {
		return projectManager;
	}

}

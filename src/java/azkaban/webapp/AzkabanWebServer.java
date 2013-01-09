/*
 * Copyright 2012 LinkedIn, Inc
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
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import azkaban.executor.ExecutorManager;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectManager;

import azkaban.scheduler.JdbcScheduleLoader;
import azkaban.scheduler.ScheduleManager;
import azkaban.security.DefaultHadoopSecurityManager;
import azkaban.security.HadoopSecurityManager;
import azkaban.user.UserManager;
import azkaban.user.XmlUserManager;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.servlet.AzkabanServletContextListener;

import azkaban.webapp.servlet.ExecutorServlet;
import azkaban.webapp.servlet.HdfsBrowserServlet;
import azkaban.webapp.servlet.ScheduleServlet;
import azkaban.webapp.servlet.HistoryServlet;
import azkaban.webapp.servlet.IndexServlet;
import azkaban.webapp.servlet.ProjectManagerServlet;
import azkaban.webapp.session.SessionCache;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

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
public class AzkabanWebServer implements AzkabanServer {
	private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);

	public static final String AZKABAN_HOME = "AZKABAN_HOME";
	public static final String DEFAULT_CONF_PATH = "conf";
	public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
	public static final String AZKABAN_PRIVATE_PROPERTIES_FILE = "azkaban.private.properties";
	public static final String JDO_PROPERTIES_FILE = "jdo.properties";

	private static final int MAX_FORM_CONTENT_SIZE = 10*1024*1024;
	private static AzkabanWebServer app;

	private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";
	// private static final int DEFAULT_PORT_NUMBER = 8081;
	private static final int DEFAULT_SSL_PORT_NUMBER = 8443;
	private static final int DEFAULT_THREAD_NUMBER = 20;
	private static final String VELOCITY_DEV_MODE_PARAM = "velocity.dev.mode";
	private static final String USER_MANAGER_CLASS_PARAM = "user.manager.class";
	private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
	private static final String DEFAULT_STATIC_DIR = "";

	private final VelocityEngine velocityEngine;

	private UserManager userManager;
	private ProjectManager projectManager;
	private ExecutorManager executorManager;
	private ScheduleManager scheduleManager;

	private final ClassLoader baseClassLoader;
	private HadoopSecurityManager hadoopSecurityManager;
	
	private Props props;
	private SessionCache sessionCache;
	private File tempDir;

	/**
	 * Constructor usually called by tomcat AzkabanServletContext to create the
	 * initial server
	 */
	public AzkabanWebServer() throws Exception {
		this(loadConfigurationFromAzkabanHome());
	}

	/**
	 * Constructor
	 */
	public AzkabanWebServer(Props props) throws Exception {
		this.props = props;
		velocityEngine = configureVelocityEngine(props.getBoolean(VELOCITY_DEV_MODE_PARAM, false));
		sessionCache = new SessionCache(props);
		userManager = loadUserManager(props);
		projectManager = loadProjectManager(props);
		executorManager = loadExecutorManager(props);
		scheduleManager = loadScheduleManager(executorManager, props);
		baseClassLoader = getBaseClassloader();
		hadoopSecurityManager = loadHadoopSecurityManager(props);
		
		tempDir = new File(props.getString("azkaban.temp.dir", "temp"));

		// Setup time zone
		if (props.containsKey(DEFAULT_TIMEZONE_ID)) {
			String timezone = props.getString(DEFAULT_TIMEZONE_ID);
			TimeZone.setDefault(TimeZone.getTimeZone(timezone));
			DateTimeZone.setDefault(DateTimeZone.forID(timezone));

			logger.info("Setting timezone to " + timezone);
		}
	}
	
	private UserManager loadUserManager(Props props) {
		Class<?> userManagerClass = props.getClass(USER_MANAGER_CLASS_PARAM, null);
		logger.info("Loading user manager class " + userManagerClass.getName());
		UserManager manager = null;

		if (userManagerClass != null && userManagerClass.getConstructors().length > 0) {

			try {
				Constructor<?> userManagerConstructor = userManagerClass.getConstructor(Props.class);
				manager = (UserManager) userManagerConstructor.newInstance(props);
			} 
			catch (Exception e) {
				logger.error("Could not instantiate UserManager "+ userManagerClass.getName());
				throw new RuntimeException(e);
			}

		} 
		else {
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
		ExecutorManager execManager = new ExecutorManager(props, loader);
		return execManager;
	}

	private ScheduleManager loadScheduleManager(ExecutorManager execManager, Props props ) throws Exception {
		ScheduleManager schedManager = new ScheduleManager(execManager, projectManager, new JdbcScheduleLoader(props));

		return schedManager;
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
	
	/**
	 * Creates and configures the velocity engine.
	 * 
	 * @param devMode
	 * @return
	 */
	private VelocityEngine configureVelocityEngine(final boolean devMode) {
		VelocityEngine engine = new VelocityEngine();
		engine.setProperty("resource.loader", "classpath");
		engine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
		engine.setProperty("classpath.resource.loader.cache", !devMode);
		engine.setProperty("classpath.resource.loader.modificationCheckInterval", 5L);
		engine.setProperty("resource.manager.logwhenfound", false);
		engine.setProperty("input.encoding", "UTF-8");
		engine.setProperty("output.encoding", "UTF-8");
		engine.setProperty("directive.set.null.allowed", true);
		engine.setProperty("resource.manager.logwhenfound", false);
		engine.setProperty("velocimacro.permissions.allow.inline", true);
		engine.setProperty("velocimacro.library.autoreload", devMode);
		engine.setProperty("velocimacro.library", "/azkaban/webapp/servlet/velocity/macros.vm");
		engine.setProperty("velocimacro.permissions.allow.inline.to.replace.global", true);
		engine.setProperty("velocimacro.arguments.strict", true);
		engine.setProperty("runtime.log.invalid.references", devMode);
		engine.setProperty("runtime.log.logsystem.class", Log4JLogChute.class);
		engine.setProperty("runtime.log.logsystem.log4j.logger", Logger.getLogger("org.apache.velocity.Logger"));
		engine.setProperty("parser.pool.size", 3);
		return engine;
	}

	private ClassLoader getBaseClassloader() throws MalformedURLException {
		final ClassLoader retVal;

		String hadoopHome = System.getenv("HADOOP_HOME");
		String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");

		if (hadoopConfDir != null) {
			logger.info("Using hadoop config found in " + hadoopConfDir);
			retVal = new URLClassLoader(new URL[] { new File(hadoopConfDir)
					.toURI().toURL() }, getClass().getClassLoader());
		} else if (hadoopHome != null) {
			logger.info("Using hadoop config found in " + hadoopHome);
			retVal = new URLClassLoader(
					new URL[] { new File(hadoopHome, "conf").toURI().toURL() },
					getClass().getClassLoader());
		} else {
			logger.info("HADOOP_HOME not set, using default hadoop config.");
			retVal = getClass().getClassLoader();
		}

		return retVal;
	}

	public HadoopSecurityManager getHadoopSecurityManager() {
		return hadoopSecurityManager;
	}
	
	private HadoopSecurityManager loadHadoopSecurityManager(Props props) {
		
		Class<?> hadoopSecurityManagerClass = props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, null);
		logger.info("Loading hadoop security manager class " + hadoopSecurityManagerClass.getName());
		HadoopSecurityManager hadoopSecurityManager = null;

		if (hadoopSecurityManagerClass != null && hadoopSecurityManagerClass.getConstructors().length > 0) {

			try {
				Constructor<?> hsmConstructor = hadoopSecurityManagerClass.getConstructor(Props.class);
				hadoopSecurityManager = (HadoopSecurityManager) hsmConstructor.newInstance(props);
			} 
			catch (Exception e) {
				logger.error("Could not instantiate Hadoop Security Manager "+ hadoopSecurityManagerClass.getName());
				throw new RuntimeException(e);
			}
		} 
		else {
			hadoopSecurityManager = new DefaultHadoopSecurityManager();
		}

		return hadoopSecurityManager;

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
		OptionParser parser = new OptionParser();

		OptionSpec<String> configDirectory = parser
				.acceptsAll(Arrays.asList("c", "conf"), "The conf directory for Azkaban.")
				.withRequiredArg()
				.describedAs("conf").ofType(String.class);

		logger.error("Starting Jetty Azkaban...");

		// Grabbing the azkaban settings from the conf directory.
		Props azkabanSettings = null;
		OptionSet options = parser.parse(args);
		if (options.has(configDirectory)) {
			String path = options.valueOf(configDirectory);
			logger.info("Loading azkaban settings file from " + path);
			File dir = new File(path);
			if (!dir.exists()) {
				logger.error("Conf directory " + path + " doesn't exist.");
			}
			else if (!dir.isDirectory()) {
				logger.error("Conf directory " + path + " isn't a directory.");
			}
			else {
				azkabanSettings = loadAzkabanConfigurationFromDirectory(dir);
			}
		} 
		else {
			logger.info("Conf parameter not set, attempting to get value from AZKABAN_HOME env.");
			azkabanSettings = loadConfigurationFromAzkabanHome();
		}

		if (azkabanSettings == null) {
			logger.error("Azkaban Properties not loaded.");
			logger.error("Exiting Azkaban...");
			return;
		}
		app = new AzkabanWebServer(azkabanSettings);

		// int portNumber =
		// azkabanSettings.getInt("jetty.port",DEFAULT_PORT_NUMBER);
		int sslPortNumber = azkabanSettings.getInt("jetty.ssl.port",
				DEFAULT_SSL_PORT_NUMBER);
		int maxThreads = azkabanSettings.getInt("jetty.maxThreads",
				DEFAULT_THREAD_NUMBER);

		logger.info("Setting up Jetty Server with port:" + sslPortNumber + " and numThreads:" + maxThreads);

		final Server server = new Server();
		SslSocketConnector secureConnector = new SslSocketConnector();
		secureConnector.setPort(sslPortNumber);
		secureConnector.setKeystore(azkabanSettings.getString("jetty.keystore"));
		secureConnector.setPassword(azkabanSettings.getString("jetty.password"));
		secureConnector.setKeyPassword(azkabanSettings.getString("jetty.keypassword"));
		secureConnector.setTruststore(azkabanSettings.getString("jetty.truststore"));
		secureConnector.setTrustPassword(azkabanSettings.getString("jetty.trustpassword"));
		
		server.addConnector(secureConnector);

		QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
		server.setThreadPool(httpThreadPool);

		String staticDir = azkabanSettings.getString("web.resource.dir", DEFAULT_STATIC_DIR);
		logger.info("Setting up web resource dir " + staticDir);
		Context root = new Context(server, "/", Context.SESSIONS);
		root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);
		
		root.setResourceBase(staticDir);
		ServletHolder index = new ServletHolder(new IndexServlet());
		root.addServlet(index, "/index");
		root.addServlet(index, "/");

		ServletHolder staticServlet = new ServletHolder(new DefaultServlet());
		root.addServlet(staticServlet, "/css/*");
		root.addServlet(staticServlet, "/js/*");
		root.addServlet(staticServlet, "/images/*");
		root.addServlet(staticServlet, "/favicon.ico");
		
		root.addServlet(new ServletHolder(new ProjectManagerServlet()),"/manager");
		root.addServlet(new ServletHolder(new ExecutorServlet()),"/executor");
		root.addServlet(new ServletHolder(new HistoryServlet()), "/history");
		root.addServlet(new ServletHolder(new ScheduleServlet()),"/schedule");
		root.addServlet(new ServletHolder(new HdfsBrowserServlet()), "/hdfs/*");
		
		root.setAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY, app);

		try {
			server.start();
		} 
		catch (Exception e) {
			logger.warn(e);
			Utils.croak(e.getMessage(), 1);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				logger.info("Shutting down http server...");
				try {
					server.stop();
					server.destroy();
				} 
				catch (Exception e) {
					logger.error("Error while shutting down http server.", e);
				}
				logger.info("kk thx bye.");
			}
		});
		logger.info("Server running on port " + sslPortNumber + ".");
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

		if (!new File(azkabanHome).isDirectory() || !new File(azkabanHome).canRead()) {
			logger.error(azkabanHome + " is not a readable directory.");
			return null;
		}

		File confPath = new File(azkabanHome, DEFAULT_CONF_PATH);
		if (!confPath.exists() || !confPath.isDirectory()
				|| !confPath.canRead()) {
			logger.error(azkabanHome + " does not contain a readable conf directory.");
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
}

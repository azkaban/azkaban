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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
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

import azkaban.executor.ExecutorLoader;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.jmx.JmxFlowRunnerManager;
import azkaban.jmx.JmxJettyServer;
import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectLoader;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanServer;
import azkaban.webapp.servlet.AzkabanServletContextListener;

public class AzkabanExecutorServer {
	private static final Logger logger = Logger.getLogger(AzkabanExecutorServer.class);
	private static final int MAX_FORM_CONTENT_SIZE = 10*1024*1024;

	public static final String AZKABAN_HOME = "AZKABAN_HOME";
	public static final String DEFAULT_CONF_PATH = "conf";
	public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
	public static final String AZKABAN_PRIVATE_PROPERTIES_FILE = "azkaban.private.properties";
	public static final String JOBTYPE_PLUGIN_DIR = "azkaban.jobtype.plugin.dir";
	public static final int DEFAULT_PORT_NUMBER = 12321;
	
	private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";
	private static final int DEFAULT_THREAD_NUMBER = 50;

	private static AzkabanExecutorServer app;
	
	private ExecutorLoader executionLoader;
	private ProjectLoader projectLoader;
	private FlowRunnerManager runnerManager;
	private Props props;
	private Props executorGlobalProps;
	private Server server;
	
	private ArrayList<ObjectName> registeredMBeans = new ArrayList<ObjectName>();
	private MBeanServer mbeanServer;

	/**
	 * Constructor
	 * 
	 * @throws Exception
	 */
	public AzkabanExecutorServer(Props props) throws Exception {
		this.props = props;

		int portNumber = props.getInt("executor.port", DEFAULT_PORT_NUMBER);
		int maxThreads = props.getInt("executor.maxThreads", DEFAULT_THREAD_NUMBER);

		server = new Server(portNumber);
		QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
		server.setThreadPool(httpThreadPool);

		Context root = new Context(server, "/", Context.SESSIONS);
		root.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);
		
		root.addServlet(new ServletHolder(new ExecutorServlet()), "/executor");
		root.addServlet(new ServletHolder(new JMXHttpServlet()), "/jmx");
		root.setAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY, this);
		
		
		executionLoader = createExecLoader(props);
		projectLoader = createProjectLoader(props);
		runnerManager = new FlowRunnerManager(props, executionLoader, projectLoader, this.getClass().getClassLoader());
		
		String globalPropsPath = props.getString("executor.global.properties", null);
		if (globalPropsPath == null) {
			executorGlobalProps = new Props();
		}
		else {
			executorGlobalProps = new Props(null, globalPropsPath);
		}
		runnerManager.setGlobalProps(executorGlobalProps);
		
		configureMBeanServer();

		try {
			server.start();
		} 
		catch (Exception e) {
			logger.warn(e);
			Utils.croak(e.getMessage(), 1);
		}
		
		logger.info("Azkaban Executor Server started on port " + portNumber);
	}

	private ExecutorLoader createExecLoader(Props props) {
		return new JdbcExecutorLoader(props);
	}
	
	private ProjectLoader createProjectLoader(Props props) {
		return new JdbcProjectLoader(props);
	}
	
	public void stopServer() throws Exception {
		server.stop();
		server.destroy();
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
	
	public Props getExecutorGlobalProps() {
		return executorGlobalProps;
	}

	/**
	 * Azkaban using Jetty
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		logger.error("Starting Jetty Azkaban Executor...");
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

	public FlowRunnerManager getFlowRunnerManager() {
		return runnerManager;
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

		registerMbean("executorJetty", new JmxJettyServer(server));
		registerMbean("flowRunnerManager", new JmxFlowRunnerManager(runnerManager));
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

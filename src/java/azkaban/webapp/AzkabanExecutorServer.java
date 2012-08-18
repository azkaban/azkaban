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
import java.util.Arrays;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import azkaban.executor.FlowRunnerManager;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.servlet.AzkabanServletContextListener;
import azkaban.webapp.servlet.ExecutorServlet;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class AzkabanExecutorServer {
	private static final Logger logger = Logger.getLogger(AzkabanExecutorServer.class);

	public static final String AZKABAN_HOME = "AZKABAN_HOME";
	public static final String DEFAULT_CONF_PATH = "conf";
	public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
	public static final int DEFAULT_PORT_NUMBER = 12321;
	
	private static final String DEFAULT_TIMEZONE_ID = "default.timezone.id";
	private static final int DEFAULT_THREAD_NUMBER = 50;

	private static AzkabanExecutorServer app;
	
	private FlowRunnerManager runnerManager;
	private Props props;
	private File tempDir;
	private Server server;

	/**
	 * Constructor
	 * 
	 * @throws Exception
	 */
	public AzkabanExecutorServer(Props props) throws Exception {
		this.props = props;

		int portNumber = props.getInt("executor.port", DEFAULT_PORT_NUMBER);
		int maxThreads = props.getInt("executor.maxThreads",
				DEFAULT_THREAD_NUMBER);

		Server server = new Server(portNumber);
		QueuedThreadPool httpThreadPool = new QueuedThreadPool(maxThreads);
		server.setThreadPool(httpThreadPool);

        Context root = new Context(server, "/", Context.SESSIONS);
		String sharedToken = props.getString("executor.shared.token", "");

		ServletHolder executorHolder = new ServletHolder(new ExecutorServlet(sharedToken));
		root.addServlet(executorHolder, "/executor");
		root.setAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY, this);
		runnerManager = new FlowRunnerManager(props);
		
		try {
			server.start();
		} 
		catch (Exception e) {
			logger.warn(e);
			Utils.croak(e.getMessage(), 1);
		}
		
		logger.info("Azkaban Executor Server started on port " + portNumber);

		tempDir = new File(props.getString("azkaban.temp.dir", "temp"));
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
		OptionParser parser = new OptionParser();

		OptionSpec<String> configDirectory = parser
				.acceptsAll(Arrays.asList("c", "conf"),
						"The conf directory for Azkaban.").withRequiredArg()
				.describedAs("conf").ofType(String.class);

		logger.error("Starting Jetty Azkaban Executor...");

		// Grabbing the azkaban settings from the conf directory.
		Props azkabanSettings = null;
		OptionSet options = parser.parse(args);
		if (options.has(configDirectory)) {
			String path = options.valueOf(configDirectory);
			logger.info("Loading azkaban settings file from " + path);
			File file = new File(path, AZKABAN_PROPERTIES_FILE);
			if (!file.exists() || file.isDirectory() || !file.canRead()) {
				logger.error("Cannot read file " + file);
			}

			azkabanSettings = loadAzkabanConfiguration(file.getPath());
		} else {
			logger.info("Conf parameter not set, attempting to get value from AZKABAN_HOME env.");
			azkabanSettings = loadConfigurationFromAzkabanHome();
		}

		if (azkabanSettings == null) {
			logger.error("Azkaban Properties not loaded.");
			logger.error("Exiting Azkaban Executor Server...");
			return;
		}

		// Setup time zone
		if (azkabanSettings.containsKey(DEFAULT_TIMEZONE_ID)) {
			String timezone = azkabanSettings.getString(DEFAULT_TIMEZONE_ID);
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
		if (!confPath.exists() || !confPath.isDirectory()
				|| !confPath.canRead()) {
			logger.error(azkabanHome
					+ " does not contain a readable conf directory.");
			return null;
		}

		File confFile = new File(confPath, AZKABAN_PROPERTIES_FILE);
		if (!confFile.exists() || confFile.isDirectory() || !confPath.canRead()) {
			logger.error(confFile + " does not contain a readable azkaban.properties file.");
			return null;
		}

		return loadAzkabanConfiguration(confFile.getPath());
	}

	public FlowRunnerManager getFlowRunnerManager() {
		return runnerManager;
	}
	
	/**
	 * Returns the set temp dir
	 * 
	 * @return
	 */
	public File getTempDirectory() {
		return tempDir;
	}

	/**
	 * Loads the Azkaban conf file int a Props object
	 * 
	 * @param path
	 * @return
	 */
	private static Props loadAzkabanConfiguration(String path) {
		try {
			return new Props(null, path);
		} catch (FileNotFoundException e) {
			logger.error("File not found. Could not load azkaban config file "
					+ path);
		} catch (IOException e) {
			logger.error("File found, but error reading. Could not load azkaban config file "
					+ path);
		}

		return null;
	}
}

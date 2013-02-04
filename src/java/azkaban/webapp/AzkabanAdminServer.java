package azkaban.webapp;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Arrays;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;

import azkaban.user.UserManager;
import azkaban.user.XmlUserManager;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.servlet.AzkabanServletContextListener;
import azkaban.webapp.servlet.admin.InitialSetupServlet;
import azkaban.webapp.session.SessionCache;

public class AzkabanAdminServer implements AzkabanServer {
	private static final Logger logger = Logger.getLogger(AzkabanAdminServer.class);
	private static AzkabanAdminServer app;
	
	private static final String AZKABAN_DEFAULT_ADMIN_DIR = "admin";
	private static final String AZKABAN_DEFAULT_ADMIN_PROPERTIES = "admin.properties";
	private static final String AZKABAN_DEFAULT_WEB_DIR = "web";
	private static final String AZKABAN_DEFAULT_PLUGIN_DIR = "pluginlib";
	private static final int AZKABAN_DEFAULT_ADMIN_PORT = 12233;
	
	private static final String USER_MANAGER_CLASS_PARAM = "user.manager.class";
	private final VelocityEngine velocityEngine;
	@SuppressWarnings("unused")
	private final SessionCache sessionCache;
	private static Server server;
	private String pluginLibDirectory;
	private File adminPropertiesFile;

	private Props props;
	
	public AzkabanAdminServer(Props props, File propFile, boolean initialSetup) throws Exception {
		this.props = props;
		velocityEngine = configureVelocityEngine(true);
		sessionCache = initialSetup ? null : new SessionCache(props);
		adminPropertiesFile = propFile;
		
		pluginLibDirectory = props.get("pluginLibDirectory");
	}
	
	public File getPropFile() {
		return adminPropertiesFile;
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
	
	/**
	 * Azkaban using Jetty
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		OptionParser parser = new OptionParser();

		OptionSpec<Integer> portArg = parser
				.acceptsAll(Arrays.asList("p", "port"), "The port for azkaban.")
				.withRequiredArg()
				.describedAs("port").ofType(Integer.class);
		
		OptionSpec<String> adminDirectory = parser
				.acceptsAll(Arrays.asList("d", "dir"), "The admin directory")
				.withRequiredArg()
				.describedAs("dir").ofType(String.class);

		OptionSpec<String> webDirectory = parser
				.acceptsAll(Arrays.asList("w", "web"), "The web resource directory")
				.withRequiredArg()
				.describedAs("web").ofType(String.class);
		
		OptionSpec<String> pluginLibDirectory = parser
				.acceptsAll(Arrays.asList("l", "pluginlib"), "The plugin lib directory")
				.withRequiredArg()
				.describedAs("plugin").ofType(String.class);
		
		OptionSet options = parser.parse(args);

		String adminDir = options.has(adminDirectory) ? options.valueOf(adminDirectory) : AZKABAN_DEFAULT_ADMIN_DIR;
		int port = options.has(portArg) ? options.valueOf(portArg) : AZKABAN_DEFAULT_ADMIN_PORT;
		String webDir = options.has(webDirectory) ? options.valueOf(webDirectory) : AZKABAN_DEFAULT_WEB_DIR;
		String pluginLib = options.has(pluginLibDirectory) ? options.valueOf(pluginLibDirectory) : AZKABAN_DEFAULT_PLUGIN_DIR;
		
		Props props = null;
		server = new Server();
		File adminDirFile = new File(adminDir);
		if (!adminDirFile.exists()) {
			adminDirFile.mkdirs();
		}
		File adminPropertiesFile = new File(adminDirFile, AZKABAN_DEFAULT_ADMIN_PROPERTIES);

		boolean secureMode = false;
		if (adminPropertiesFile.exists()) {
			props = new Props(null, adminPropertiesFile);
			if (props.containsKey("mysql.host") && props.containsKey("user.manager.class")) {
				secureMode = true;
			}
		}
		else {
			props = new Props();
		}

		if (secureMode) {
			app = new AzkabanAdminServer(props, adminPropertiesFile, false);
			
			SslSocketConnector secureConnector = new SslSocketConnector();
			secureConnector.setPort(port);
			server.addConnector(secureConnector);
		}
		else {
			logger.info("Server settings not found. Running setup wizard.");
			props.put("pluginLibDirectory", pluginLib);
			app = new AzkabanAdminServer(props, adminPropertiesFile, true);
			SocketConnector connector = new SocketConnector();
			connector.setPort(port);
			server.addConnector(connector);
		}
		
		Context root = new Context(server, "/", Context.SESSIONS);
		String staticDir = webDir;
		root.setResourceBase(staticDir);
		
		root.setAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY, app);
		ServletHolder executorHolder = new ServletHolder(new InitialSetupServlet());
		root.addServlet(executorHolder, "/");
	
		ServletHolder staticServlet = new ServletHolder(new DefaultServlet());
		root.addServlet(staticServlet, "/css/*");
		root.addServlet(staticServlet, "/js/*");
		root.addServlet(staticServlet, "/images/*");
		root.addServlet(staticServlet, "/favicon.ico");
		
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
					app.stopServer();
				} catch (Exception e) {
					logger.error("Error while shutting down http server.", e);
				}
				logger.info("kk thx bye.");
			}
		});
	}
	
	public void stopServer() throws Exception {
		server.stop();
		server.destroy();
	}

	@Override
	public Props getServerProps() {
		return props;
	}

	@Override
	public VelocityEngine getVelocityEngine() {
		return velocityEngine;
	}

	@Override
	public SessionCache getSessionCache() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserManager getUserManager() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String getPluginLibDirectory() {
		return pluginLibDirectory;
	}

	@SuppressWarnings("unused")
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
}

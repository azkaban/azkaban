/*
 * Copyright 2020 LinkedIn Corp.
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
 *
 */
package azkaban.database;

import azkaban.client.AzkabanClient;
import azkaban.AzkabanCommonModule;
import azkaban.client.CreateProjectResponse;
import azkaban.client.ScheduleCronResponse;
import azkaban.client.FlowExecution;
import azkaban.client.FlowExecutionResponse;
import azkaban.client.FlowExecutions;
import azkaban.client.UnscheduleCronResponse;
import azkaban.client.UploadProjectResponse;
import azkaban.db.AzkabanDataSource;
import azkaban.db.DatabaseSetup;
import azkaban.execapp.AzkabanExecServerModule;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.executor.Status;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.user.XmlUserManager;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.AzkabanWebServerModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_GLOBAL_PROPERTIES_EXT_PATH;
import static azkaban.Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzkabanPostgresTest {

	private static final Props props = new Props();

	private static final int DB_PORT = 54320;
	private static final String JETTY_PORT = "54321";
	private static final String SERVER_PORT = "54322";
	private static final String EXEC_PORT = "54323";
	private static final String BUILD_PATH = "./build/";
	private static final String INSTALL_PATH = BUILD_PATH + "install/";
	private static final String PROJECT_NAME = "test";
	private static final String PROJ_DESC = "proj desc";
	private static final String FLOW_NAME = "basic";
	private static final String SUCCEEDED = Status.SUCCEEDED.name();

	private static AzkabanClient client;
	private static EmbeddedPostgres embeddedDb;
	private static Injector injector;
	private static CompletableFuture<Void> webServer;
	private static CompletableFuture<Void> execServer;

	// wait for executor to show up in the database
	private static void waitForExecutor() throws SQLException, InterruptedException {
		AzkabanDataSource ads = injector.getInstance(AzkabanDataSource.class);
		String sql = "SELECT * FROM executors";
		boolean hasExecutors = false;
		System.out.print("waiting for executor");
		do {
			System.out.print(".");
			try (Connection conn = ads.getConnection();
					 PreparedStatement ps = conn.prepareStatement(sql);
					 ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					hasExecutors = true;
					System.out.print("\n");
				}
			}
			Thread.sleep(1000);
		} while (!hasExecutors);
	}

	private static void activateExecutor() throws URISyntaxException, IOException {

		URIBuilder builder =
				new URIBuilder(String.format("http://localhost:%s/executor", EXEC_PORT))
						.addParameter("action", "activate");

		HttpGet get = new HttpGet(builder.build());

		get.addHeader("accept", "application/json");

		try (CloseableHttpClient httpClient = HttpClients.createDefault();
				 CloseableHttpResponse response = httpClient.execute(get)) {
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				throw new RuntimeException("invalid HTTP status code");
			}
		}

	}

	@BeforeClass
	public static void setUp() throws Exception {

		embeddedDb = EmbeddedPostgres.builder().setPort(DB_PORT).start();

		DataSource ds = embeddedDb.getPostgresDatabase();

		File scriptsDir = new File("../azkaban-db/build/sql");

		File[] files = scriptsDir.listFiles((dir, name) -> name.startsWith("create-all-postgresql"));

		if (files == null || files.length != 1) {
			Assert.fail("failed to read postgres SQL file");
		}

		try (Connection conn = ds.getConnection()) {
			conn.setAutoCommit(false);
			DatabaseSetup.runTableScript(conn, files[0]);
		}

		props.put("executor.port", EXEC_PORT);
		props.put("azkaban.webserver.url", "http://localhost:" + JETTY_PORT);
		props.put(AZKABAN_GLOBAL_PROPERTIES_EXT_PATH,
				INSTALL_PATH + "azkaban-web-server/conf/global.properties");
		props.put("azkaban.execution.dir", BUILD_PATH + "executions");
		props.put("azkaban.project.dir", BUILD_PATH + "projects");
		props.put("web.resource.dir", INSTALL_PATH + "azkaban-web-server/web");

		props.put("database.type", "postgresql");
		props.put("postgresql.port", Integer.toString(DB_PORT));
		props.put("postgresql.database", "postgres");
		props.put("postgresql.host", "localhost");
		props.put("postgresql.user", "postgres");
		props.put("postgresql.password", "postgres");
		props.put("postgresql.numconnections", "100");

		props.put(USE_MULTIPLE_EXECUTORS, "true");
		props.put("server.port", SERVER_PORT);
		props.put("jetty.port", JETTY_PORT);
		props.put("server.useSSL", "true");
		props.put("jetty.use.ssl", "false");
		props.put("user.manager.xml.file",
				INSTALL_PATH + "azkaban-web-server/conf/azkaban-users.xml");

		props.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
		props.put("org.quartz.threadPool.threadCount", "10");
		props.put("default.timezone.id", "US/Pacific");
		props.put("azkaban.jobtype.plugin.dir", BUILD_PATH + "jobtypes");
		props.put("postgresql.url.suffix", "");

		injector = getInjector();

		webServer = CompletableFuture.runAsync(() -> {
			try {
				AzkabanExecutorServer.launch(injector.getInstance(AzkabanExecutorServer.class));
			} catch (Exception e) {
				Assert.fail(e.getMessage());
			}
		});

		waitForExecutor();

		activateExecutor();

		execServer = CompletableFuture.runAsync(() -> {
			try {
				AzkabanWebServer.launch(injector.getInstance(AzkabanWebServer.class));
			} catch (Exception e) {
				Assert.fail(e.getMessage());
			}
		});

		String url = "http://localhost:" + JETTY_PORT;

		client = new AzkabanClient(url, "azkaban", "azkaban", 10);

	}

	@AfterClass
	public static void tearDown() throws IOException {

		AzkabanWebServer aws = injector.getInstance(AzkabanWebServer.class);
		aws.close();

		// FileWatcher does not close properly on shutdown, so we close manually
		XmlUserManager um = (XmlUserManager) aws.getUserManager();
		um.close();

		AzkabanExecutorServer aes = injector.getInstance(AzkabanExecutorServer.class);

		try {
			aes.shutdownNow();
		} catch (Exception e) {
			//no-op
		}

		embeddedDb.close();

		client.close();

		CompletableFuture.allOf(webServer, execServer).join();

		SERVICE_PROVIDER.unsetInjector();

		deleteQuietly(new File(BUILD_PATH + "executions"));
		deleteQuietly(new File(BUILD_PATH + "projects"));

	}

	protected static Injector getInjector() {

		final Injector injector = Guice.createInjector(
				new AzkabanCommonModule(props),
				new AzkabanWebServerModule(props),
				new AzkabanExecServerModule());

		SERVICE_PROVIDER.unsetInjector();
		SERVICE_PROVIDER.setInjector(injector);

		return injector;
	}


	@Test
	public void test1CreateDeleteProject() throws Exception {

		CreateProjectResponse cpr = client.createProject(PROJECT_NAME, PROJ_DESC);
		assertEquals("success", cpr.getStatus());

		ProjectManager projectManager = injector.getInstance(ProjectManager.class);

		assertNotNull(projectManager.getProject(PROJECT_NAME));

		client.deleteProject(PROJECT_NAME);

		assertNull(projectManager.getProject(PROJECT_NAME));

		client.createProject(PROJECT_NAME, PROJ_DESC);

		assertNotNull(projectManager.getProject(PROJECT_NAME));

	}

	@Test
	public void test2UploadProject() throws Exception {

		ProjectManager projectManager = injector.getInstance(ProjectManager.class);

		File basicFlow20 = new File("../az-examples/flow20-projects/basicFlow20Project.zip");

		UploadProjectResponse upr = client.uploadProjectZip(PROJECT_NAME, basicFlow20);

		assertNotNull(upr.getProjectId());

		Project project = projectManager.getProject(PROJECT_NAME);

		assertEquals(1, project.getFlows().size());

	}

	@Test
	public void test3ExecuteFlow() throws Exception {
		FlowExecutionResponse fer = client.executeFlow(PROJECT_NAME, FLOW_NAME);
		assertEquals("1", fer.getExecId());
	}

	@Test
	public void test4ScheduleUnscheduleFlow() throws Exception {

		DateTime dateTime = new DateTime();
		DateTime newTime = dateTime.plusMinutes(1);
		int hour = newTime.getHourOfDay();
		int min = newTime.getMinuteOfHour();

		// schedule flow execution one minute in future
		String cronExp = String.format("0 %d %d * * ?", min, hour);

		ScheduleCronResponse fer = client.scheduleFlow(PROJECT_NAME, FLOW_NAME, cronExp);

		assertEquals("success", fer.getStatus());

		int timeoutMs = 120000;
		long numSuccess = 0;
		FlowExecutions executions = null;

		// wait for two successful flow executions
		while (numSuccess < 2 && new DateTime().getMillis() - dateTime.getMillis() < timeoutMs) {
			executions = client.getFlowExecutions(PROJECT_NAME, FLOW_NAME, 2);
			numSuccess = executions.getExecutions().stream()
					.filter(e -> e.getStatus().equals(SUCCEEDED)).count();
			Thread.sleep(1000);
		}

		assertNotNull(executions);

		List<FlowExecution> execs = executions.getExecutions();
		assertEquals(2, execs.size());

		for (FlowExecution exec : execs) {
			assertEquals(SUCCEEDED, exec.getStatus());
		}

		UnscheduleCronResponse ucr = client.unscheduleFlow(fer.getScheduleId());

		assertEquals("success", ucr.getStatus());

	}

}

/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.webapp;

import static azkaban.Constants.ConfigurationKeys.JETTY_PORT;
import static azkaban.Constants.ConfigurationKeys.JETTY_USE_SSL;
import static azkaban.Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS;
import static azkaban.Constants.DEFAULT_EXECUTOR_PORT_FILE;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static azkaban.ServiceProviderTest.assertSingleton;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.AzkabanCommonModule;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.ContainerizedExecutionManagerProperties;
import azkaban.DispatchMethod;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.db.DatabaseOperator;
import azkaban.db.H2FileDataSource;
import azkaban.executor.ActiveExecutingFlowsDao;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutionFlowDao;
import azkaban.executor.ExecutionJobDao;
import azkaban.executor.ExecutionLogsDao;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorDao;
import azkaban.executor.ExecutorEventsDao;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.FetchActiveFlowDao;
import azkaban.executor.container.KubernetesContainerizedImpl;
import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManager;
import azkaban.scheduler.QuartzScheduler;
import azkaban.spi.Storage;
import azkaban.trigger.TriggerLoader;
import azkaban.trigger.TriggerManager;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.Answer;


public class AzkabanWebServerTest {

  public static final String AZKABAN_DB_SQL_PATH = "azkaban-db/src/main/sql";

  private static final Props props = new Props();

  private static String getUserManagerXmlFile() {
    final URL resource = AzkabanWebServerTest.class.getClassLoader()
        .getResource("azkaban-users.xml");
    return requireNonNull(resource).getPath();
  }

  private static String getSqlScriptsDir() throws IOException {
    // Dummy because any resource file works.
    final String dummyResourcePath = getUserManagerXmlFile();
    final Path resources = Paths.get(dummyResourcePath).getParent();
    final Path azkabanRoot = resources.getParent().getParent().getParent().getParent();

    final File sqlScriptDir = Paths.get(azkabanRoot.toString(), AZKABAN_DB_SQL_PATH).toFile();
    return sqlScriptDir.getCanonicalPath();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tearDown();

    final String sqlScriptsDir = getSqlScriptsDir();
    props.put(AzkabanDatabaseSetup.DATABASE_SQL_SCRIPT_DIR, sqlScriptsDir);

    props.put("database.type", "h2");
    props.put("h2.path", "./h2");

    props.put(USE_MULTIPLE_EXECUTORS, "true");
    props.put("server.port", "0");
    props.put(JETTY_PORT, "0");
    props.put("server.useSSL", "true");
    props.put(JETTY_USE_SSL, "false");
    props.put("user.manager.xml.file", getUserManagerXmlFile());

    // Quartz settings
    props.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
    props.put("org.quartz.threadPool.threadCount", "10");
    AzkabanDatabaseUpdater.runDatabaseUpdater(props, sqlScriptsDir, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    SERVICE_PROVIDER.unsetInjector();

    deleteQuietly(new File("h2.mv.db"));
    deleteQuietly(new File("h2.trace.db"));
    deleteQuietly(new File(DEFAULT_EXECUTOR_PORT_FILE));
    deleteQuietly(new File("executions"));
    deleteQuietly(new File("projects"));
  }

  @Test
  public void testInjection() throws Exception {
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(props)
    );
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(injector);

    final ExecutorLoader executorLoader = injector.getInstance(ExecutorLoader.class);
    assertNotNull(executorLoader);

    final Executor executor = executorLoader.addExecutor("localhost", 60000);
    executor.setActive(true);
    executorLoader.updateExecutor(executor);

    assertNotNull(injector.getInstance(ExecutionFlowDao.class));
    assertNotNull(injector.getInstance(DatabaseOperator.class));

    //Test if triggermanager is singletonly guiced. If not, the below test will fail.
    assertSingleton(ExecutorLoader.class, injector);
    assertSingleton(ExecutorManagerAdapter.class, injector);
    assertSingleton(ProjectLoader.class, injector);
    assertSingleton(ProjectManager.class, injector);
    assertSingleton(Storage.class, injector);
    assertSingleton(TriggerLoader.class, injector);
    assertSingleton(TriggerManager.class, injector);
    assertSingleton(AlerterHolder.class, injector);
    assertSingleton(Emailer.class, injector);
    assertSingleton(ExecutionFlowDao.class, injector);
    assertSingleton(ExecutorDao.class, injector);
    assertSingleton(ExecutionJobDao.class, injector);
    assertSingleton(ExecutionLogsDao.class, injector);
    assertSingleton(ExecutorEventsDao.class, injector);
    assertSingleton(ActiveExecutingFlowsDao.class, injector);
    assertSingleton(FlowTriggerScheduler.class, injector);
    assertSingleton(FetchActiveFlowDao.class, injector);
    assertSingleton(AzkabanWebServer.class, injector);
    assertSingleton(H2FileDataSource.class, injector);

    assertSingleton(QuartzScheduler.class, injector);

    SERVICE_PROVIDER.unsetInjector();
  }

  @Test
  public void testDispatchMethod() throws ExecutorManagerException {
    // Test for PUSH method
    props.put(ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD, DispatchMethod.PUSH.name());
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(props)
    );
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(injector);

    final ExecutorManagerAdapter executorManagerAdapter =
        injector.getInstance(ExecutorManagerAdapter.class);
    assertNotNull(executorManagerAdapter);
    assertEquals(executorManagerAdapter.getDispatchMethod(), DispatchMethod.PUSH);

    // Test for POLL method
    props.put(ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,"POLL");
    final Injector pollInjector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(props)
    );
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(pollInjector);

    final ExecutorManagerAdapter pollExecutorManagerAdapter =
        pollInjector.getInstance(ExecutorManagerAdapter.class);
    assertNotNull(pollExecutorManagerAdapter);
    assertEquals(pollExecutorManagerAdapter.getDispatchMethod(), DispatchMethod.POLL);

    // Test for CONTAINERIZED method
    props.put(ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,"CONTAINERIZED");
    props.put(ContainerizedExecutionManagerProperties.KUBERNETES_KUBE_CONFIG_PATH, "src/test"
        + "/resources/container/kubeconfig");
    props.put(ContainerizedExecutionManagerProperties.KUBERNETES_NAMESPACE, "dev-namespace");
    final Injector containerizedInjector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(props)
    );
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(containerizedInjector);

    final ExecutorManagerAdapter containerizedInjectorInstance =
        containerizedInjector.getInstance(ExecutorManagerAdapter.class);
    assertNotNull(containerizedInjectorInstance);
    assertEquals(containerizedInjectorInstance.getDispatchMethod(), DispatchMethod.CONTAINERIZED);

    // Test default method
    props.put(ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,"UNKNOWN");
    final Injector defaultInjector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(props)
    );
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(defaultInjector);

    final ExecutorManagerAdapter defaultInjectorInstance =
        defaultInjector.getInstance(ExecutorManagerAdapter.class);
    assertNotNull(defaultInjectorInstance);
    assertEquals(defaultInjectorInstance.getDispatchMethod(), DispatchMethod.PUSH);
  }
}

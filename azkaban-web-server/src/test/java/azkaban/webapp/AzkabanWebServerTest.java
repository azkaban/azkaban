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

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
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

import static azkaban.ServiceProvider.*;
import static azkaban.executor.ExecutorManager.*;
import static java.util.Objects.*;
import static org.apache.commons.io.FileUtils.*;
import static org.junit.Assert.*;


public class AzkabanWebServerTest {
  public static final String AZKABAN_DB_SQL_PATH = "azkaban-db/src/main/sql";

  private static final Props props = new Props();

  private static String getUserManagerXmlFile() {
    URL resource = AzkabanWebServerTest.class.getClassLoader().getResource("azkaban-users.xml");
    return requireNonNull(resource).getPath();
  }

  private static String getSqlScriptsDir() throws IOException {
    // Dummy because any resource file works.
    final String dummyResourcePath = getUserManagerXmlFile();
    Path resources = Paths.get(dummyResourcePath).getParent();
    Path azkabanRoot = resources.getParent().getParent().getParent().getParent();

    File sqlScriptDir = Paths.get(azkabanRoot.toString(), AZKABAN_DB_SQL_PATH).toFile();
    return sqlScriptDir.getCanonicalPath();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tearDown();

    String sqlScriptsDir = getSqlScriptsDir();
    props.put(AzkabanDatabaseSetup.DATABASE_SQL_SCRIPT_DIR, sqlScriptsDir);

    props.put("database.type", "h2");
    props.put("h2.path", "./h2");

    props.put(AZKABAN_USE_MULTIPLE_EXECUTORS, "true");
    props.put("server.port", "0");
    props.put("jetty.port", "0");
    props.put("server.useSSL", "true");
    props.put("jetty.use.ssl", "false");
    props.put("user.manager.xml.file", getUserManagerXmlFile());

    AzkabanDatabaseUpdater.runDatabaseUpdater(props, sqlScriptsDir, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    SERVICE_PROVIDER.unsetInjector();

    deleteQuietly(new File("h2.mv.db"));
    deleteQuietly(new File("h2.trace.db"));
    deleteQuietly(new File("executor.port"));
    deleteQuietly(new File("executions"));
    deleteQuietly(new File("projects"));
  }

  @Test
  public void testInjection() throws Exception {
    Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule()
    );
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(injector);

    ExecutorLoader executorLoader = injector.getInstance(ExecutorLoader.class);
    assertNotNull(executorLoader);

    Executor executor = executorLoader.addExecutor("localhost", 60000);
    executor.setActive(true);
    executorLoader.updateExecutor(executor);

    assertNotNull(injector.getInstance(AzkabanWebServer.class));
    SERVICE_PROVIDER.unsetInjector();
  }
}

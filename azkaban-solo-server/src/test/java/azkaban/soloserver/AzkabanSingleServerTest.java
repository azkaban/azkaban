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

package azkaban.soloserver;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.execapp.AzkabanExecServerModule;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServerModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;


public class AzkabanSingleServerTest {

  public static final String AZKABAN_DB_SQL_PATH = "azkaban-db/src/main/sql";
  private static final Props props = new Props();

  private static String getConfPath() {
    final URL resource = AzkabanSingleServerTest.class.getClassLoader().getResource("conf");
    return requireNonNull(resource).getPath();
  }

  private static String getSqlScriptsDir() throws IOException {
    // Dummy because any resource file works.
    final Path resources = Paths.get(getConfPath()).getParent();
    final Path azkabanRoot = resources.getParent().getParent().getParent().getParent();

    final File sqlScriptDir = Paths.get(azkabanRoot.toString(), AZKABAN_DB_SQL_PATH).toFile();
    return sqlScriptDir.getCanonicalPath();
  }

  @AfterClass
  public static void tearDown() {
    deleteQuietly(new File("h2.mv.db"));
    deleteQuietly(new File("h2.trace.db"));
    deleteQuietly(new File(Constants.DEFAULT_EXECUTOR_PORT_FILE));
    deleteQuietly(new File("executions"));
    deleteQuietly(new File("projects"));
  }

  @Before
  public void setUp() throws Exception {
    tearDown();

    final String confPath = getConfPath();

    props.put("database.type", "h2");
    props.put("h2.path", "./h2");

    props.put(Constants.ConfigurationKeys.USE_MULTIPLE_EXECUTORS, "true");
    props.put("server.port", "0");
    props.put("jetty.port", "0");
    props.put("server.useSSL", "true");
    props.put("jetty.use.ssl", "false");
    props.put("user.manager.xml.file", new File(confPath, "azkaban-users.xml").getPath());
    props.put(ConfigurationKeys.EXECUTOR_PORT, "12321");

    // Quartz settings
    props.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
    props.put("org.quartz.threadPool.threadCount", "10");

    final String sqlScriptsDir = getSqlScriptsDir();
    assertTrue(new File(sqlScriptsDir).isDirectory());
    props.put(AzkabanDatabaseSetup.DATABASE_SQL_SCRIPT_DIR, sqlScriptsDir);
    AzkabanDatabaseUpdater.runDatabaseUpdater(props, sqlScriptsDir, true);
  }

  @Test
  public void testInjection() {
    SERVICE_PROVIDER.unsetInjector();
    /* Initialize Guice Injector */
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanExecServerModule(),
        new AzkabanWebServerModule()
    );
    SERVICE_PROVIDER.setInjector(injector);

    /* Launch server */
    assertNotNull(injector.getInstance(AzkabanSingleServer.class));

    SERVICE_PROVIDER.unsetInjector();
  }
}

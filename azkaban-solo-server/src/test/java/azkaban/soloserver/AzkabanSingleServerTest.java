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

import azkaban.AzkabanCommonModule;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.execapp.AzkabanExecServerModule;
import azkaban.server.AzkabanServer;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServerModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import java.net.URL;
import org.apache.log4j.Logger;
import org.junit.Test;

import static azkaban.ServiceProvider.*;
import static java.util.Objects.*;
import static org.junit.Assert.*;


public class AzkabanSingleServerTest {
  private static final Logger log = Logger.getLogger(AzkabanSingleServerTest.class);

  private String getConfPath() {
    final URL resource = AzkabanSingleServerTest.class.getClassLoader().getResource("conf");
    return requireNonNull(resource).getPath();
  }
  @Test
  public void testInjection() throws Exception {
    Props props = AzkabanServer.loadProps(new String[] {"-c", "conf", getConfPath() });
    assertNotNull(props);

    if (props.getBoolean(AzkabanDatabaseSetup.DATABASE_CHECK_VERSION, true)) {
      boolean updateDB = props.getBoolean(AzkabanDatabaseSetup.DATABASE_AUTO_UPDATE_TABLES, true);
      String scriptDir = props.getString(AzkabanDatabaseSetup.DATABASE_SQL_SCRIPT_DIR, "sql");
      AzkabanDatabaseUpdater.runDatabaseUpdater(props, scriptDir, updateDB);
    }

    /* Initialize Guice Injector */
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(),
        new AzkabanExecServerModule()
    );
    SERVICE_PROVIDER.setInjector(injector);

    /* Launch server */
    assertNotNull(injector.getInstance(AzkabanSingleServer.class));
  }
}

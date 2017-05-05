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

package azkaban.soloserver;

import azkaban.AzkabanCommonModule;
import azkaban.execapp.AzkabanExecServerModule;
import azkaban.webapp.AzkabanWebServerModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.log4j.Logger;

import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.server.AzkabanServer;
import azkaban.webapp.AzkabanWebServer;
import azkaban.utils.Props;

import static azkaban.ServiceProvider.*;


public class AzkabanSingleServer {
  private static final Logger log = Logger.getLogger(AzkabanWebServer.class);

  private final AzkabanWebServer webServer;
  private final AzkabanExecutorServer executor;

  @Inject
  public AzkabanSingleServer(AzkabanWebServer webServer, AzkabanExecutorServer executor) {
    this.webServer = webServer;
    this.executor = executor;
  }

  private void launch() throws Exception {
    AzkabanWebServer.launch(webServer);
    log.info("Azkaban Web Server started...");

    AzkabanExecutorServer.launch(executor);
    log.info("Azkaban Exec Server started...");
  }

  public static void main(String[] args) throws Exception {
    log.info("Starting Azkaban Server");

    Props props = AzkabanServer.loadProps(args);
    if (props == null) {
      log.error("Properties not found. Need it to connect to the db.");
      log.error("Exiting...");
      return;
    }

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
    injector.getInstance(AzkabanSingleServer.class).launch();
  }
}

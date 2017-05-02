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
import azkaban.utils.StdOutErrRedirect;
import azkaban.webapp.AzkabanWebServerModule;
import com.google.inject.Guice;
import java.io.IOException;
import java.sql.SQLException;
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

  public static void main(String[] args) {
    // Redirect all std out and err messages into log4j
    StdOutErrRedirect.redirectOutAndErrToLog();

    log.info("Starting Azkaban Server");

    Props props = AzkabanServer.loadProps(args);
    if (props == null) {
      log.error("Properties not found. Need it to connect to the db.");
      log.error("Exiting...");
      System.exit(1);
    }

    if (props.getBoolean(AzkabanDatabaseSetup.DATABASE_CHECK_VERSION, true)) {
      boolean updateDB = props.getBoolean(AzkabanDatabaseSetup.DATABASE_AUTO_UPDATE_TABLES, true);
      String scriptDir = props.getString(AzkabanDatabaseSetup.DATABASE_SQL_SCRIPT_DIR, "sql");
      try {
        AzkabanDatabaseUpdater.runDatabaseUpdater(props, scriptDir, updateDB);
      } catch (IOException | SQLException e) {
        log.error("Error setting up DB. Exiting..", e);
        System.exit(1);
      }
    }

    /* Initialize Guice Injector */
    SERVICE_PROVIDER.setInjector(Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(),
        new AzkabanExecServerModule()
    ));

    try {
      AzkabanWebServer.launch(props);
      log.info("Azkaban Web Server started...");
    } catch (Exception e) {
      log.error("Web Server start failed. Exiting...", e);
      System.exit(1);
    }

    try {
      AzkabanExecutorServer.launch(props);
      log.info("Azkaban Exec Server started...");
    } catch (Exception e) {
      log.error("Exec Server start failed. Exiting...", e);
      System.exit(1);
    }
  }
}

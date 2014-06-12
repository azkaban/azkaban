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

import org.apache.log4j.Logger;

import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.server.AzkabanServer;
import azkaban.webapp.AzkabanWebServer;
import azkaban.utils.Props;

public class AzkabanSingleServer {
  private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);

  public static void main(String[] args) throws Exception {
    logger.info("Starting Azkaban Server");

    Props props = AzkabanServer.loadProps(args);
    if (props == null) {
      logger.error("Properties not found. Need it to connect to the db.");
      logger.error("Exiting...");
      return;
    }

    boolean checkversion =
        props.getBoolean(AzkabanDatabaseSetup.DATABASE_CHECK_VERSION, true);

    if (checkversion) {
      boolean updateDB =
          props.getBoolean(AzkabanDatabaseSetup.DATABASE_AUTO_UPDATE_TABLES,
              true);
      String scriptDir =
          props.getString(AzkabanDatabaseSetup.DATABASE_SQL_SCRIPT_DIR, "sql");
      AzkabanDatabaseUpdater.runDatabaseUpdater(props, scriptDir, updateDB);
    }

    AzkabanWebServer.main(args);
    logger.info("Azkaban Web Server started...");
    AzkabanExecutorServer.main(args);
    logger.info("Azkaban Exec Server started...");
  }
}

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

package azkaban.database;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.log4j.Logger;

import azkaban.database.AzkabanDatabaseSetup;
import azkaban.utils.Props;
import azkaban.server.AzkabanServer;

public class AzkabanDatabaseUpdater {
  private static final Logger logger = Logger
      .getLogger(AzkabanDatabaseUpdater.class);

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();

    OptionSpec<String> scriptDirectory =
        parser
            .acceptsAll(Arrays.asList("s", "script"),
                "Directory of update scripts.").withRequiredArg()
            .describedAs("script").ofType(String.class);

    OptionSpec<Void> updateOption =
        parser.acceptsAll(Arrays.asList("u", "update"),
            "Will update if necessary");

    Props props = AzkabanServer.loadProps(args, parser);

    if (props == null) {
      logger.error("Properties not found. Need it to connect to the db.");
      logger.error("Exiting...");
      return;
    }

    OptionSet options = parser.parse(args);
    boolean updateDB = false;
    if (options.has(updateOption)) {
      updateDB = true;
    } else {
      logger.info("Running DatabaseUpdater in test mode");
    }

    String scriptDir = "sql";
    if (options.has(scriptDirectory)) {
      scriptDir = options.valueOf(scriptDirectory);
    }

    runDatabaseUpdater(props, scriptDir, updateDB);
  }

  public static void runDatabaseUpdater(Props props, String sqlDir,
      boolean updateDB) throws IOException, SQLException {
    logger.info("Use scripting directory " + sqlDir);

    if (updateDB) {
      logger.info("Will auto update any changes.");
    } else {
      logger.info("Running DatabaseUpdater in test mode. Use -u to update");
    }

    AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(props);
    setup.loadTableInfo();
    if (!setup.needsUpdating()) {
      logger.info("Everything looks up to date.");
      return;
    }

    logger.info("Need to update the db.");
    setup.printUpgradePlan();

    if (updateDB) {
      logger.info("Updating DB");
      setup.updateDatabase(true, true);
    }
  }
}

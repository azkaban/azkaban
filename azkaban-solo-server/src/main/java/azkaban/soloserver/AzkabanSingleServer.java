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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.execapp.AzkabanExecServerModule;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.server.AzkabanServer;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.AzkabanWebServerModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import java.io.IOException;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class AzkabanSingleServer {

  private static final Logger log = Logger.getLogger(AzkabanWebServer.class);

  private final AzkabanWebServer webServer;
  private final AzkabanExecutorServer executor;

  @Inject
  public AzkabanSingleServer(final AzkabanWebServer webServer,
      final AzkabanExecutorServer executor) {
    this.webServer = webServer;
    this.executor = executor;
  }

  public static void main(final String[] args) {
    try {
      start(args);
    } catch (final Exception e) {
      log.error("Failed to start single server. Shutting down.", e);
      System.exit(1);
    }
  }

  private static void initializeQuartzDb(final String jdbcUrl, final String user,
      final String password, final String initScript) {
    try {
      DriverManager.registerDriver(
          (Driver) Class.forName("org.h2.Driver").getDeclaredConstructor().newInstance());
      Properties connectionProps = new Properties();
      connectionProps.put("user", user);
      connectionProps.put("password", password);
      Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
      String tableName = "QRTZ_TRIGGERS";
      boolean tableExists = conn.getMetaData().getTables(null, null, tableName, null).next();
      if (tableExists) {
        log.info("Quartz DB already initialized");
      } else {
        File initScriptFile = new File(initScript);
        String sql = "RUNSCRIPT FROM '" + initScriptFile.getAbsolutePath() + "'";
        try {
          conn.createStatement().execute(sql);
          log.info("Quartz DB initialized");
        } catch (SQLException e) {
          log.info("Error running sql initializer: " + sql, e);
        }
      }
    } catch (Exception e) {
      log.warn("Error initializing Quartz DB", e);
    }
    new File("local/plugins").mkdirs();
  }

  public static void start(String[] args) throws Exception {
    log.info("Starting Azkaban Server");

    if (System.getSecurityManager() == null) {
      Policy.setPolicy(new Policy() {
        @Override
        public boolean implies(final ProtectionDomain domain, final Permission permission) {
          return true; // allow all
        }
      });
      System.setSecurityManager(new SecurityManager());
    }

    if (args.length == 0) {
      args = prepareDefaultConf();
    }

    final Props props = AzkabanServer.loadProps(args);
    if (props == null) {
      log.error("Properties not found. Need it to connect to the db.");
      log.error("Exiting...");
      return;
    }

    if (props.getBoolean(Constants.ConfigurationKeys.ENABLE_QUARTZ, true)) {
      String dataSource = props.getString("org.quartz.jobStore.dataSource");
      String initScript = props.getString("azkaban.server.schedule.quartz_schema");
      if (dataSource != null) {
        String tablePrefix = props.getString("org.quartz.jobStore.tablePrefix");
        if ("QRTZ_".equals(tablePrefix)) {
          String driver = props.getString("org.quartz.dataSource." + dataSource + ".driver");
          if ("org.h2.Driver".equals(driver)) {
            String jdbcUrl = props.getString("org.quartz.dataSource." + dataSource + ".URL");
            String user = props.getString("org.quartz.dataSource." + dataSource + ".user");
            String password = props.getString("org.quartz.dataSource." + dataSource + ".password");
            initializeQuartzDb(jdbcUrl, user, password, initScript);
          }
        }
      }
    }

    if (props.getBoolean(AzkabanDatabaseSetup.DATABASE_CHECK_VERSION, true)) {
      final boolean updateDB = props
          .getBoolean(AzkabanDatabaseSetup.DATABASE_AUTO_UPDATE_TABLES, true);
      final String scriptDir = props.getString(AzkabanDatabaseSetup.DATABASE_SQL_SCRIPT_DIR, "sql");
      AzkabanDatabaseUpdater.runDatabaseUpdater(props, scriptDir, updateDB);
    }

    /* Initialize Guice Injector */
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(props),
        new AzkabanExecServerModule()
    );
    SERVICE_PROVIDER.setInjector(injector);

    /* Launch server */
    injector.getInstance(AzkabanSingleServer.class).launch();
  }

  /**
   * To enable "run out of the box for testing".
   */
  private static String[] prepareDefaultConf() throws IOException {
    final File templateFolder = new File("test/local-conf-templates");
    final File localConfFolder = new File("local/conf");
    if (!localConfFolder.exists()) {
      FileUtils.copyDirectory(templateFolder, localConfFolder.getParentFile());
      log.info("Copied local conf templates from " + templateFolder.getAbsolutePath());
    }
    log.info("Using conf at " + localConfFolder.getAbsolutePath());
    return new String[]{"-conf", "local/conf"};
  }

  private void launch() throws Exception {
    // exec server first so that it's ready to accept calls by web server when web initializes
    AzkabanExecutorServer.launch(this.executor);
    log.info("Azkaban Exec Server started...");

    this.executor.getFlowRunnerManager()
        .setExecutorActive(true, this.executor.getHost(), this.executor.getPort());
    log.info("Azkaban Exec Server activated...");

    AzkabanWebServer.launch(this.webServer);
    log.info("Azkaban Web Server started...");
  }
}

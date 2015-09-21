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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import azkaban.database.DataSourceUtils.PropertyType;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;

public class AzkabanDatabaseSetup {
  private static final Logger logger = Logger
      .getLogger(AzkabanDatabaseSetup.class);
  public static final String DATABASE_CHECK_VERSION = "database.check.version";
  public static final String DATABASE_AUTO_UPDATE_TABLES =
      "database.auto.update.tables";
  public static final String DATABASE_SQL_SCRIPT_DIR =
      "database.sql.scripts.dir";

  private static final String DEFAULT_SCRIPT_PATH = "sql";
  private static final String CREATE_SCRIPT_PREFIX = "create.";
  private static final String UPDATE_SCRIPT_PREFIX = "update.";
  private static final String SQL_SCRIPT_SUFFIX = ".sql";

  private static String FETCH_PROPERTY_BY_TYPE =
      "SELECT name, value FROM properties WHERE type=?";
  private static final String INSERT_DB_PROPERTY =
      "INSERT INTO properties (name, type, value, modified_time) values (?,?,?,?)";
  private static final String UPDATE_DB_PROPERTY =
      "UPDATE properties SET value=?,modified_time=? WHERE name=? AND type=?";

  private AzkabanDataSource dataSource;
  private Map<String, String> tables;
  private Map<String, String> installedVersions;
  private Set<String> missingTables;
  private Map<String, List<String>> upgradeList;
  private Props dbProps;
  private String version;
  private boolean needsUpdating;

  private String scriptPath = null;

  public AzkabanDatabaseSetup(Props props) {
    this(DataSourceUtils.getDataSource(props));
    this.scriptPath =
        props.getString(DATABASE_SQL_SCRIPT_DIR, DEFAULT_SCRIPT_PATH);
  }

  public AzkabanDatabaseSetup(AzkabanDataSource ds) {
    this.dataSource = ds;
    if (scriptPath == null) {
      scriptPath = DEFAULT_SCRIPT_PATH;
    }
  }

  public void loadTableInfo() throws IOException, SQLException {
    tables = new HashMap<String, String>();
    installedVersions = new HashMap<String, String>();
    missingTables = new HashSet<String>();
    upgradeList = new HashMap<String, List<String>>();

    dbProps = loadDBProps();
    version = dbProps.getString("version");

    loadInstalledTables();
    loadTableVersion();
    findMissingTables();
    findOutOfDateTables();

    needsUpdating = !upgradeList.isEmpty() || !missingTables.isEmpty();
  }

  public boolean needsUpdating() {
    if (version == null) {
      throw new RuntimeException("Uninitialized. Call loadTableInfo first.");
    }

    return needsUpdating;
  }

  public void printUpgradePlan() {
    if (!tables.isEmpty()) {
      logger.info("The following are installed tables");
      for (Map.Entry<String, String> installedTable : tables.entrySet()) {
        logger.info(" " + installedTable.getKey() + " version:"
            + installedTable.getValue());
      }
    } else {
      logger.info("No installed tables found.");
    }

    if (!missingTables.isEmpty()) {
      logger.info("The following are missing tables that need to be installed");
      for (String table : missingTables) {
        logger.info(" " + table);
      }
    } else {
      logger.info("There are no missing tables.");
    }

    if (!upgradeList.isEmpty()) {
      logger.info("The following tables need to be updated.");
      for (Map.Entry<String, List<String>> upgradeTable : upgradeList
          .entrySet()) {
        String tableInfo = " " + upgradeTable.getKey() + " versions:";
        for (String upVersion : upgradeTable.getValue()) {
          tableInfo += upVersion + ",";
        }

        logger.info(tableInfo);
      }
    } else {
      logger.info("No tables need to be updated.");
    }
  }

  public void updateDatabase(boolean createTable, boolean updateTable)
      throws SQLException, IOException {
    // We call this because it has an unitialize check.
    if (!needsUpdating()) {
      logger.info("Nothing to be done.");
      return;
    }

    if (createTable && !missingTables.isEmpty()) {
      createNewTables();
    }
    if (updateTable && !upgradeList.isEmpty()) {
      updateTables();
    }
  }

  private Props loadDBProps() throws IOException {
    File dbPropsFile = new File(this.scriptPath, "database.properties");

    if (!dbPropsFile.exists()) {
      throw new IOException("Cannot find 'database.properties' file in "
          + dbPropsFile.getPath());
    }

    Props props = new Props(null, dbPropsFile);
    return props;
  }

  private void loadTableVersion() throws SQLException {
    logger.info("Searching for table versions in the properties table");
    if (tables.containsKey("properties")) {
      // Load version from settings
      QueryRunner runner = new QueryRunner(dataSource);
      Map<String, String> map =
          runner.query(FETCH_PROPERTY_BY_TYPE, new PropertiesHandler(),
              PropertyType.DB.getNumVal());
      for (String key : map.keySet()) {
        String value = map.get(key);
        if (key.endsWith(".version")) {
          String tableName =
              key.substring(0, key.length() - ".version".length());
          installedVersions.put(tableName, value);
          if (tables.containsKey(tableName)) {
            tables.put(tableName, value);
          }
        }
      }
    } else {
      logger.info("Properties table doesn't exist.");
    }
  }

  private void loadInstalledTables() throws SQLException {
    logger.info("Searching for installed tables");
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      ResultSet rs =
          conn.getMetaData().getTables(conn.getCatalog(), null, null,
              new String[] { "TABLE" });

      while (rs.next()) {
        tables.put(rs.getString("TABLE_NAME").toLowerCase(), "2.1");
      }
    } finally {
      DbUtils.commitAndCloseQuietly(conn);
    }
  }

  private void findMissingTables() {
    File directory = new File(scriptPath);
    File[] createScripts =
        directory.listFiles(new FileIOUtils.PrefixSuffixFileFilter(
            CREATE_SCRIPT_PREFIX, SQL_SCRIPT_SUFFIX));

    for (File script : createScripts) {
      String name = script.getName();
      String[] nameSplit = name.split("\\.");
      String tableName = nameSplit[1];

      if (!tables.containsKey(tableName)) {
        missingTables.add(tableName);
      }
    }
  }

  private void findOutOfDateTables() {
    for (String key : tables.keySet()) {
      String version = tables.get(key);

      List<String> upgradeVersions = findOutOfDateTable(key, version);
      if (upgradeVersions != null && !upgradeVersions.isEmpty()) {
        upgradeList.put(key, upgradeVersions);
      }
    }
    for (String key : missingTables) {
      List<String> upgradeVersions = findOutOfDateTable(key, "");
      if (upgradeVersions != null && !upgradeVersions.isEmpty()) {
        upgradeList.put(key, upgradeVersions);
      }
    }
  }

  private List<String> findOutOfDateTable(String table, String currentVersion) {
    File directory = new File(scriptPath);
    ArrayList<String> versions = new ArrayList<String>();

    File[] createScripts =
        directory.listFiles(new FileIOUtils.PrefixSuffixFileFilter(
            UPDATE_SCRIPT_PREFIX + table, SQL_SCRIPT_SUFFIX));
    if (createScripts.length == 0) {
      return null;
    }

    String updateFileNameVersion = UPDATE_SCRIPT_PREFIX + table + "." + currentVersion;
    for (File file : createScripts) {
      String fileName = file.getName();
      if (fileName.compareTo(updateFileNameVersion) > 0) {
        String[] split = fileName.split("\\.");
        String updateScriptVersion = "";

        for (int i = 2; i < split.length - 1; ++i) {
          try {
            Integer.parseInt(split[i]);
            updateScriptVersion += split[i] + ".";
          } catch (NumberFormatException e) {
            break;
          }
        }
        if (updateScriptVersion.endsWith(".")) {
          updateScriptVersion = updateScriptVersion.substring(0, updateScriptVersion.length() - 1);

          // add to update list if updateScript will update above current
          // version and upto targetVersion in database.properties
          if (updateScriptVersion.compareTo(currentVersion) > 0
            && updateScriptVersion.compareTo(this.version) <= 0) {
            versions.add(updateScriptVersion);
          }
        }
      }
    }

    Collections.sort(versions);
    return versions;
  }

  private void createNewTables() throws SQLException, IOException {
    Connection conn = dataSource.getConnection();
    conn.setAutoCommit(false);
    try {
      // Make sure that properties table is created first.
      if (missingTables.contains("properties")) {
        runTableScripts(conn, "properties", version, dataSource.getDBType(),
            false);
      }
      for (String table : missingTables) {
        if (!table.equals("properties")) {
          runTableScripts(conn, table, version, dataSource.getDBType(), false);
          // update version as we have create a new table
          installedVersions.put(table, version);
        }
      }
    } finally {
      conn.close();
    }
  }

  private void updateTables() throws SQLException, IOException {
    Connection conn = dataSource.getConnection();
    conn.setAutoCommit(false);
    try {
      // Make sure that properties table is created first.
      if (upgradeList.containsKey("properties")) {
        for (String version : upgradeList.get("properties")) {
          runTableScripts(conn, "properties", version, dataSource.getDBType(),
              true);
        }
      }
      for (String table : upgradeList.keySet()) {
        if (!table.equals("properties")) {
          for (String version : upgradeList.get(table)) {
            runTableScripts(conn, table, version, dataSource.getDBType(), true);
          }
        }
      }
    } finally {
      conn.close();
    }
  }

  private void runTableScripts(Connection conn, String table, String version,
      String dbType, boolean update) throws IOException, SQLException {
    String scriptName = "";
    if (update) {
      scriptName = "update." + table + "." + version;
      logger.info("Update table " + table + " to version " + version);
    } else {
      scriptName = "create." + table;
      logger.info("Creating new table " + table + " version " + version);
    }

    String dbSpecificScript = scriptName + "." + dbType + ".sql";

    File script = new File(scriptPath, dbSpecificScript);
    if (!script.exists()) {
      String dbScript = scriptName + ".sql";
      script = new File(scriptPath, dbScript);

      if (!script.exists()) {
        throw new IOException("Creation files do not exist for table " + table);
      }
    }

    BufferedInputStream buff = null;
    try {
      buff = new BufferedInputStream(new FileInputStream(script));
      String queryStr = IOUtils.toString(buff);

      String[] splitQuery = queryStr.split(";\\s*\n");

      QueryRunner runner = new QueryRunner();

      for (String query : splitQuery) {
        runner.update(conn, query);
      }

      // If it's properties, then we want to commit the table before we update
      // it
      if (table.equals("properties")) {
        conn.commit();
      }

      String propertyName = table + ".version";
      if (!installedVersions.containsKey(table)) {
        runner.update(conn, INSERT_DB_PROPERTY, propertyName,
            DataSourceUtils.PropertyType.DB.getNumVal(), version,
            System.currentTimeMillis());
      } else {
        runner.update(conn, UPDATE_DB_PROPERTY, version,
            System.currentTimeMillis(), propertyName,
            DataSourceUtils.PropertyType.DB.getNumVal());
      }
      conn.commit();
    } finally {
      IOUtils.closeQuietly(buff);
    }
  }

  public static class PropertiesHandler implements
      ResultSetHandler<Map<String, String>> {
    @Override
    public Map<String, String> handle(ResultSet rs) throws SQLException {
      Map<String, String> results = new HashMap<String, String>();
      while (rs.next()) {
        String key = rs.getString(1);
        String value = rs.getString(2);
        results.put(key, value);
      }

      return results;
    }
  }
}

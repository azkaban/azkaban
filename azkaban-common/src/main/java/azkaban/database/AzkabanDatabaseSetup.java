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

import azkaban.database.DataSourceUtils.PropertyType;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
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

/**
 * @deprecated in favor of {@link azkaban.db.DatabaseSetup}.
 */
@Deprecated
public class AzkabanDatabaseSetup {

  public static final String DATABASE_CHECK_VERSION = "database.check.version";
  public static final String DATABASE_AUTO_UPDATE_TABLES =
      "database.auto.update.tables";
  public static final String DATABASE_SQL_SCRIPT_DIR =
      "database.sql.scripts.dir";
  private static final Logger logger = Logger
      .getLogger(AzkabanDatabaseSetup.class);
  private static final String DEFAULT_SCRIPT_PATH = "sql";
  private static final String CREATE_SCRIPT_PREFIX = "create.";
  private static final String UPDATE_SCRIPT_PREFIX = "update.";
  private static final String SQL_SCRIPT_SUFFIX = ".sql";

  private static final String FETCH_PROPERTY_BY_TYPE =
      "SELECT name, value FROM properties WHERE type=?";
  private static final String INSERT_DB_PROPERTY =
      "INSERT INTO properties (name, type, value, modified_time) values (?,?,?,?)";
  private static final String UPDATE_DB_PROPERTY =
      "UPDATE properties SET value=?,modified_time=? WHERE name=? AND type=?";

  private final AzkabanDataSource dataSource;
  private Map<String, String> tables;
  private Map<String, String> installedVersions;
  private Set<String> missingTables;
  private Map<String, List<String>> upgradeList;
  private String version;
  private boolean needsUpdating;

  private String scriptPath = null;

  public AzkabanDatabaseSetup(final Props props) {
    this(DataSourceUtils.getDataSource(props));
    this.scriptPath =
        props.getString(DATABASE_SQL_SCRIPT_DIR, DEFAULT_SCRIPT_PATH);
  }

  public AzkabanDatabaseSetup(final AzkabanDataSource ds) {
    this.dataSource = ds;
    if (this.scriptPath == null) {
      this.scriptPath = DEFAULT_SCRIPT_PATH;
    }
  }

  // TODO kunkun-tang: Refactor this class. loadTableInfo method should sit inside constructor
  public AzkabanDatabaseSetup(final AzkabanDataSource ds, final Props props) {
    this.dataSource = ds;
    this.scriptPath = props.getString(DATABASE_SQL_SCRIPT_DIR, DEFAULT_SCRIPT_PATH);
  }

  public void loadTableInfo() throws IOException, SQLException {
    this.tables = new HashMap<>();
    this.installedVersions = new HashMap<>();
    this.missingTables = new HashSet<>();
    this.upgradeList = new HashMap<>();

    final Props dbProps = loadDBProps();
    this.version = dbProps.getString("version");

    loadInstalledTables();
    loadTableVersion();
    findMissingTables();
    findOutOfDateTables();

    this.needsUpdating = !this.upgradeList.isEmpty() || !this.missingTables.isEmpty();
  }

  public boolean needsUpdating() {
    if (this.version == null) {
      throw new RuntimeException("Uninitialized. Call loadTableInfo first.");
    }

    return this.needsUpdating;
  }

  public void printUpgradePlan() {
    if (!this.tables.isEmpty()) {
      logger.info("The following are installed tables");
      for (final Map.Entry<String, String> installedTable : this.tables.entrySet()) {
        logger.info(" " + installedTable.getKey() + " version:"
            + installedTable.getValue());
      }
    } else {
      logger.info("No installed tables found.");
    }

    if (!this.missingTables.isEmpty()) {
      logger.info("The following are missing tables that need to be installed");
      for (final String table : this.missingTables) {
        logger.info(" " + table);
      }
    } else {
      logger.info("There are no missing tables.");
    }

    if (!this.upgradeList.isEmpty()) {
      logger.info("The following tables need to be updated.");
      for (final Map.Entry<String, List<String>> upgradeTable : this.upgradeList
          .entrySet()) {
        String tableInfo = " " + upgradeTable.getKey() + " versions:";
        for (final String upVersion : upgradeTable.getValue()) {
          tableInfo += upVersion + ",";
        }

        logger.info(tableInfo);
      }
    } else {
      logger.info("No tables need to be updated.");
    }
  }

  public void updateDatabase(final boolean createTable, final boolean updateTable)
      throws SQLException, IOException {
    // We call this because it has an unitialize check.
    if (!needsUpdating()) {
      logger.info("Nothing to be done.");
      return;
    }

    if (createTable && !this.missingTables.isEmpty()) {
      createNewTables();
    }
    if (updateTable && !this.upgradeList.isEmpty()) {
      updateTables();
    }
  }

  private Props loadDBProps() throws IOException {
    final File dbPropsFile = new File(this.scriptPath, "database.properties");

    if (!dbPropsFile.exists()) {
      throw new IOException(
          "Cannot find 'database.properties' file in " + dbPropsFile.getAbsolutePath());
    }

    return new Props(null, dbPropsFile);
  }

  private void loadTableVersion() throws SQLException {
    logger.info("Searching for table versions in the properties table");
    if (this.tables.containsKey("properties")) {
      // Load version from settings
      final QueryRunner runner = new QueryRunner(this.dataSource);
      final Map<String, String> map =
          runner.query(FETCH_PROPERTY_BY_TYPE, new PropertiesHandler(),
              PropertyType.DB.getNumVal());
      for (final String key : map.keySet()) {
        final String value = map.get(key);
        if (key.endsWith(".version")) {
          final String tableName =
              key.substring(0, key.length() - ".version".length());
          this.installedVersions.put(tableName, value);
          if (this.tables.containsKey(tableName)) {
            this.tables.put(tableName, value);
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
      conn = this.dataSource.getConnection();
      final ResultSet rs =
          conn.getMetaData().getTables(conn.getCatalog(), null, null,
              new String[]{"TABLE"});

      while (rs.next()) {
        this.tables.put(rs.getString("TABLE_NAME").toLowerCase(), "2.1");
      }
    } finally {
      DbUtils.commitAndCloseQuietly(conn);
    }
  }

  private void findMissingTables() {
    final File directory = new File(this.scriptPath);
    final File[] createScripts =
        directory.listFiles(new FileIOUtils.PrefixSuffixFileFilter(
            CREATE_SCRIPT_PREFIX, SQL_SCRIPT_SUFFIX));
    if (createScripts != null) {
      for (final File script : createScripts) {
        final String name = script.getName();
        final String[] nameSplit = name.split("\\.");
        final String tableName = nameSplit[1];

        // TODO temporary fix for Issue #1569:
        // "Startup fails: missing tables that need to be installed: quartz-tables-all"
        // this doesn't work because the file actually contains multiple tables and the file name
        // pattern doesn't match with any of those. Until this new file the convention has been that
        // each file has a single table and the file name matches the table name.
        if ("quartz-tables-all-mysql".equals(tableName)
            || "quartz-tables-all-postgresql".equals(tableName)) {
          continue;
        }
        if (!this.tables.containsKey(tableName)) {
          this.missingTables.add(tableName);
        }
      }
    }
  }

  private void findOutOfDateTables() {
    for (final String key : this.tables.keySet()) {
      final String version = this.tables.get(key);

      final List<String> upgradeVersions = findOutOfDateTable(key, version);
      if (upgradeVersions != null && !upgradeVersions.isEmpty()) {
        this.upgradeList.put(key, upgradeVersions);
      }
    }
    for (final String key : this.missingTables) {
      final List<String> upgradeVersions = findOutOfDateTable(key, "");
      if (upgradeVersions != null && !upgradeVersions.isEmpty()) {
        this.upgradeList.put(key, upgradeVersions);
      }
    }
  }

  private List<String> findOutOfDateTable(final String table, final String currentVersion) {
    final File directory = new File(this.scriptPath);
    final ArrayList<String> versions = new ArrayList<>();

    final File[] createScripts =
        directory.listFiles(new FileIOUtils.PrefixSuffixFileFilter(
            UPDATE_SCRIPT_PREFIX + table, SQL_SCRIPT_SUFFIX));
    if (createScripts == null || createScripts.length == 0) {
      return null;
    }

    final String updateFileNameVersion = UPDATE_SCRIPT_PREFIX + table + "." + currentVersion;
    for (final File file : createScripts) {
      final String fileName = file.getName();
      if (fileName.compareTo(updateFileNameVersion) > 0) {
        final String[] split = fileName.split("\\.");
        String updateScriptVersion = "";

        for (int i = 2; i < split.length - 1; ++i) {
          try {
            Integer.parseInt(split[i]);
            updateScriptVersion += split[i] + ".";
          } catch (final NumberFormatException e) {
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
    final Connection conn = this.dataSource.getConnection();
    conn.setAutoCommit(false);
    try {
      // Make sure that properties table is created first.
      if (this.missingTables.contains("properties")) {
        runTableScripts(conn, "properties", this.version, this.dataSource.getDBType(),
            false);
      }
      for (final String table : this.missingTables) {
        if (!table.equals("properties")) {
          runTableScripts(conn, table, this.version, this.dataSource.getDBType(), false);
          // update version as we have create a new table
          this.installedVersions.put(table, this.version);
        }
      }
    } finally {
      conn.close();
    }
  }

  private void updateTables() throws SQLException, IOException {
    final Connection conn = this.dataSource.getConnection();
    conn.setAutoCommit(false);
    try {
      // Make sure that properties table is created first.
      if (this.upgradeList.containsKey("properties")) {
        for (final String version : this.upgradeList.get("properties")) {
          runTableScripts(conn, "properties", version, this.dataSource.getDBType(),
              true);
        }
      }
      for (final String table : this.upgradeList.keySet()) {
        if (!table.equals("properties")) {
          for (final String version : this.upgradeList.get(table)) {
            runTableScripts(conn, table, version, this.dataSource.getDBType(), true);
          }
        }
      }
    } finally {
      conn.close();
    }
  }

  private void runTableScripts(final Connection conn, final String table, final String version,
      final String dbType, final boolean update) throws IOException, SQLException {
    String scriptName = "";
    if (update) {
      scriptName = "update." + table + "." + version;
      logger.info("Update table " + table + " to version " + version);
    } else {
      scriptName = "create." + table;
      logger.info("Creating new table " + table + " version " + version);
    }

    final String dbSpecificScript = scriptName + "." + dbType + ".sql";

    File script = new File(this.scriptPath, dbSpecificScript);
    if (!script.exists()) {
      final String dbScript = scriptName + ".sql";
      script = new File(this.scriptPath, dbScript);

      if (!script.exists()) {
        throw new IOException("Creation files do not exist for table " + table);
      }
    }

    BufferedInputStream buff = null;
    try {
      buff = new BufferedInputStream(new FileInputStream(script));
      final String queryStr = IOUtils.toString(buff);

      final String[] splitQuery = queryStr.split(";\\s*\n");

      final QueryRunner runner = new QueryRunner();

      for (final String query : splitQuery) {
        runner.update(conn, query);
      }

      // If it's properties, then we want to commit the table before we update
      // it
      if (table.equals("properties")) {
        conn.commit();
      }

      final String propertyName = table + ".version";
      if (!this.installedVersions.containsKey(table)) {
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
    public Map<String, String> handle(final ResultSet rs) throws SQLException {
      final Map<String, String> results = new HashMap<>();
      while (rs.next()) {
        final String key = rs.getString(1);
        final String value = rs.getString(2);
        results.put(key, value);
      }

      return results;
    }
  }
}

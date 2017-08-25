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
 */

package azkaban.db;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * TODO kunkun-tang: this class is quite messy now, needs to be refactored.
 */
public class DatabaseSetup {

  public static final String DATABASE_SQL_SCRIPT_DIR =
      "database.sql.scripts.dir";
  private static final Logger logger = Logger
      .getLogger(DatabaseSetup.class);
  private static final String DEFAULT_SCRIPT_PATH = "sql";
  private static final String CREATE_SCRIPT_PREFIX = "create.";
  private static final String SQL_SCRIPT_SUFFIX = ".sql";

  private static final String INSERT_DB_PROPERTY =
      "INSERT INTO properties (name, type, value, modified_time) values (?,?,?,?)";
  private static final String UPDATE_DB_PROPERTY =
      "UPDATE properties SET value=?,modified_time=? WHERE name=? AND type=?";

  private final AzkabanDataSource dataSource;
  private final Set<String> missingTables = new HashSet<>();
  private final Map<String, String> installedVersions = new HashMap<>();
  private String version;
  private boolean needsUpdating;

  private String scriptPath = null;

  public DatabaseSetup(final AzkabanDataSource ds) {
    this.dataSource = ds;
    if (this.scriptPath == null) {
      this.scriptPath = DEFAULT_SCRIPT_PATH;
    }
  }

  public DatabaseSetup(final AzkabanDataSource ds, final String path) {
    this.dataSource = ds;
    this.scriptPath = path;
  }

  public void updateDatabase()
      throws SQLException, IOException {
    findMissingTables();
    createNewTables();
  }

  private void findMissingTables() {
    final File directory = new File(this.scriptPath);
    final File[] createScripts =
        directory.listFiles(new PrefixSuffixFileFilter(
            CREATE_SCRIPT_PREFIX, SQL_SCRIPT_SUFFIX));
    if (createScripts != null) {
      for (final File script : createScripts) {
        final String name = script.getName();
        final String[] nameSplit = name.split("\\.");
        final String tableName = nameSplit[1];
        this.missingTables.add(tableName);
      }
    }
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
            1, version,
            System.currentTimeMillis());
      } else {
        runner.update(conn, UPDATE_DB_PROPERTY, version,
            System.currentTimeMillis(), propertyName, 1);
      }
      conn.commit();
    } finally {
      IOUtils.closeQuietly(buff);
    }
  }


  // Reuse code from azkaban.utils.FileIOUtils.PrefixSuffixFileFilter
  // Todo kunkun-tang: needs to be create az core modules to put this class
  public static class PrefixSuffixFileFilter implements FileFilter {

    private final String prefix;
    private final String suffix;

    public PrefixSuffixFileFilter(final String prefix, final String suffix) {
      this.prefix = prefix;
      this.suffix = suffix;
    }

    @Override
    public boolean accept(final File pathname) {
      if (!pathname.isFile() || pathname.isHidden()) {
        return false;
      }

      final String name = pathname.getName();
      final int length = name.length();
      if (this.suffix.length() > length || this.prefix.length() > length) {
        return false;
      }

      return name.startsWith(this.prefix) && name.endsWith(this.suffix);
    }
  }
}

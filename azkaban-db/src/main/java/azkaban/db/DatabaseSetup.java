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
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates DB tables. The input to this class is a folder path, which includes all create table
 * sql scripts. The script's name should follow: create.[table_name].sql in order to be
 * identified. This class is used for unit test only for now.
 *
 * <p>Todo kunkun-tang: We need to fix some reliability issues if we rely on this class to
 * create tables when launching AZ in future.
 */
public class DatabaseSetup {

  private static final Logger LOG = LoggerFactory.getLogger(DatabaseSetup.class);
  private static final String CREATE_SCRIPT_PREFIX = "create.";
  private static final String SQL_SCRIPT_SUFFIX = ".sql";

  private final AzkabanDataSource dataSource;
  private String scriptPath = null;

  public DatabaseSetup(final AzkabanDataSource ds, final String path) {
    this.dataSource = ds;
    this.scriptPath = path;
  }

  public void updateDatabase()
      throws SQLException, IOException {
    final Set<String> tables = collectAllTables();
    createTables(tables);
  }

  private Set<String> collectAllTables() {
    final Set<String> tables = new HashSet<>();
    final File directory = new File(this.scriptPath);
    final File[] createScripts =
        directory.listFiles(new PrefixSuffixFileFilter(
            CREATE_SCRIPT_PREFIX, SQL_SCRIPT_SUFFIX));
    if (createScripts != null) {
      for (final File script : createScripts) {
        final String name = script.getName();
        final String[] nameSplit = name.split("\\.");
        final String tableName = nameSplit[1];
        tables.add(tableName);
      }
    }
    return tables;
  }

  private void createTables(final Set<String> tables) throws SQLException, IOException {
    final Connection conn = this.dataSource.getConnection();
    conn.setAutoCommit(false);
    try {
      for (final String table : tables) {
        runTableScripts(conn, table);
      }
    } finally {
      conn.close();
    }
  }

  private void runTableScripts(final Connection conn, final String table)
      throws IOException, SQLException {
    LOG.info("Creating new table " + table);

    final String dbSpecificScript = "create." + table + ".sql";
    final File script = new File(this.scriptPath, dbSpecificScript);
    BufferedInputStream buff = null;
    try {
      buff = new BufferedInputStream(new FileInputStream(script));
      final String queryStr = IOUtils.toString(buff);
      final String[] splitQuery = queryStr.split(";\\s*\n");
      final QueryRunner runner = new QueryRunner();
      for (final String query : splitQuery) {
        runner.update(conn, query);
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

    PrefixSuffixFileFilter(final String prefix, final String suffix) {
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
      return this.suffix.length() <= length && this.prefix.length() <= length && name
          .startsWith(this.prefix) && name.endsWith(this.suffix);
    }
  }
}

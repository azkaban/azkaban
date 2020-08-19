/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.project;

import azkaban.db.DatabaseOperator;
import azkaban.spi.Dependency;
import azkaban.spi.FileValidationStatus;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods for interacting with dependency validation cache in DB. Used during thin archive
 * uploads.
 */
@Singleton
public class JdbcDependencyManager {
  private static final Logger log = LoggerFactory.getLogger(JdbcDependencyManager.class);

  private final DatabaseOperator dbOperator;

  @Inject
  JdbcDependencyManager(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  public Map<Dependency, FileValidationStatus> getValidationStatuses(final Set<Dependency> deps,
      final String validationKey) throws SQLException {
    Map<Dependency, FileValidationStatus> depValidationStatuses = new HashMap<>();
    if (deps.isEmpty()) {
      // There's nothing for us to do.
      return depValidationStatuses;
    }

    // Map of (filename + sha1) -> Dependency for resolving the dependencies already cached in the DB
    // after the query completes.
    Map<String, Dependency> hashAndFileNameToDep = new HashMap<>();
    Connection conn = null;
    ResultSet rs = null;
    PreparedStatement stmnt = null;

    // TODO: Use azkaban.db.DatabaseOperator.query() instead of getting the DB connection and
    // dealing with connection lifecycle.

    try {
      conn = this.dbOperator.getDataSource().getConnection();
      if (conn == null) {
        throw new SQLException("Null connection");
      }
      stmnt = conn.prepareStatement(
          String
              .format("SELECT file_name, file_sha1, validation_status FROM validated_dependencies "
                  + "WHERE validation_key = ? AND (%s)", makeStrWithQuestionMarks(deps.size())));

      // Set the first param, which is the validation_key
      stmnt.setString(1, validationKey);

      // Start at 2 because the first parameter is at index 1, and that is the validator key that we already set.
      int index = 2;
      for (Dependency d : deps) {
        stmnt.setString(index++, d.getFileName());
        stmnt.setString(index++, d.getSHA1());
        hashAndFileNameToDep.put(d.getFileName() + d.getSHA1(), d);
      }

      rs = stmnt.executeQuery();

      while (rs.next()) {
        // Columns are (starting at index 1): file_name, file_sha1, validation_status
        Dependency d = hashAndFileNameToDep.remove(rs.getString(1) + rs.getString(2));

        // HashMap.remove will return null if the key is not found, hence check for it before
        // adding to depValidationStatuses.
        if (d != null) {
          FileValidationStatus v = FileValidationStatus.valueOf(rs.getInt(3));
          depValidationStatuses.put(d, v);
        }
      }

      // All remaining dependencies in the hashToDep map should be marked as being NEW (because they weren't
      // associated with any DB entry)
      hashAndFileNameToDep.values().stream()
          .forEach(d -> depValidationStatuses.put(d, FileValidationStatus.NEW));
    } catch (final SQLException ex) {
      log.error("Transaction failed: ", ex);
      throw ex;
    } finally {
      // Replicate the order of closing in org.apache.commons.dbutils.QueryRunner#query
      DbUtils.closeQuietly(conn, stmnt, rs);
    }

    return depValidationStatuses;
  }

  public void updateValidationStatuses(final Map<Dependency, FileValidationStatus> depValidationStatuses,
      final String validationKey) throws SQLException {
    if (depValidationStatuses.isEmpty()) {
      return;
    }

    // Order of columns: file_name, file_sha1, validation_key, validation_status
    Object[][] rowsToInsert = depValidationStatuses
        .keySet()
        .stream()
        .map(d -> new Object[]{d.getFileName(), d.getSHA1(), validationKey, depValidationStatuses.get(d).getValue()})
        .toArray(Object[][]::new);

    // We use insert IGNORE because a another process may have been processing the same dependency
    // and written the row for a given dependency before we were able to (resulting in a duplicate primary key
    // error when we try to write the row), so this will ignore the error and continue persisting the other
    // dependencies.
    this.dbOperator.batch("INSERT IGNORE INTO validated_dependencies "
      + "(file_name, file_sha1, validation_key, validation_status) VALUES (?, ?, ?, ?)", rowsToInsert);
  }

  private static String makeStrWithQuestionMarks(final int num) {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < num; i++) {
      builder.append("(file_name = ? and file_sha1 = ?) or ");
    }
    // Remove trailing " or ";
    return builder.substring(0, builder.length() - 4);
  }
}

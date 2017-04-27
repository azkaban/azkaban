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
 *
 */
package azkaban.db;

import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;

/**
 * This interface is to define Base Data Access Object contract for Azkaban. All azkaban
 * DB related operations must be performed upon this interface. AZ DB operators all leverages
 * QueryRunner interface.
 *
 * @see org.apache.commons.dbutils.QueryRunner
 */
public interface DatabaseOperator {

  /**
   * Executes the given Azkaban related SELECT SQL operations.
   *
   * @param sqlQuery The SQL query statement to execute.
   * @param resultHandler The handler used to create the result object
   * @param params Initialize the PreparedStatement's IN parameters
   * @param <T> The type of object that the qeury handler returns
   * @return The object returned by the handler.
   * @throws SQLException
   */
  <T> T query(String sqlQuery, ResultSetHandler<T> resultHandler, Object...params) throws SQLException;

  /**
   * Provide a way to allow users define custom SQL operations without relying on fixed
   * SQL interface. The common use case is to group a sequence of SQL operations without
   * commit every time.
   *
   * @param operations A sequence of DB operations
   * @param <T> The type of object that the operations returns. Note that T could be null
   * @return T The object returned by the SQL statement, expected by the caller
   * @throws SQLException
   */
  <T> T transaction(SQLTransaction<T> operations) throws SQLException;

  /**
   * Executes the given AZ related INSERT, UPDATE, or DELETE SQL statement.
   *
   * @param updateClause sql statements to execute
   * @param params Initialize the PreparedStatement's IN parameters
   * @return The number of rows updated.
   * @throws SQLException
   */
  int update(String updateClause, Object...params) throws SQLException;
}

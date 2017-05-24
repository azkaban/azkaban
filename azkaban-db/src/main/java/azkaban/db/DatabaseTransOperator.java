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

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;


/**
 * This interface is designed as an supplement of {@link DatabaseOperator}, which do commit at the end of every query. Given
 * this interface, users/callers (implementation code) should decide where to {@link Connection#commit()}
 * based on their requirements.
 *
 * The diff between DatabaseTransOperator and DatabaseOperator:
 * * Auto commit and Auto close connection are enforced in DatabaseOperator, but not enabled in DatabaseTransOperator.
 * * We usually group a couple of sql operations which need the same connection into DatabaseTransOperator.
 *
 * @see org.apache.commons.dbutils.QueryRunner
 */
public interface DatabaseTransOperator {

  /**
   * returns the last id from a previous insert statement.
   * Note that last insert and this operation should use the same connection.
   *
   * @return the last inserted id in mysql per connection.
   * @throws SQLException
   */
  long getLastInsertId() throws SQLException;

  /**
   *
   * @param querySql
   * @param resultHandler
   * @param params
   * @param <T>
   * @return
   * @throws SQLException
   */
  <T> T query(String querySql, ResultSetHandler<T> resultHandler, Object... params) throws SQLException;

  /**
   *
   * @param updateClause
   * @param params
   * @return
   * @throws SQLException
   */
  int update(String updateClause, Object... params) throws SQLException;

  /**
   *
   * @return the JDBC connection associated with this operator.
   */
  Connection getConnection();
}

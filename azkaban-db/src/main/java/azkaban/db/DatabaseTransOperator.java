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
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.log4j.Logger;


/**
 * This interface is designed as an supplement of {@link DatabaseOperator}, which do commit at the
 * end of every query. Given this interface, users/callers (implementation code) should decide where
 * to {@link Connection#commit()} based on their requirements.
 *
 * The diff between DatabaseTransOperator and DatabaseOperator: * Auto commit and Auto close
 * connection are enforced in DatabaseOperator, but not enabled in DatabaseTransOperator. * We
 * usually group a couple of sql operations which need the same connection into
 * DatabaseTransOperator.
 *
 * @see org.apache.commons.dbutils.QueryRunner
 */
public class DatabaseTransOperator {

  private static final Logger logger = Logger.getLogger(DatabaseTransOperator.class);
  private final Connection conn;
  private final QueryRunner queryRunner;

  public DatabaseTransOperator(final QueryRunner queryRunner, final Connection conn) {
    this.conn = conn;
    this.queryRunner = queryRunner;
  }

  /**
   * returns the last id from a previous insert statement. Note that last insert and this operation
   * should use the same connection.
   *
   * @return the last inserted id in mysql per connection.
   */
  public long getLastInsertId() throws SQLException {
    // A default connection: autocommit = true.
    long num = -1;
    try {
      num = ((Number) this.queryRunner
          .query(this.conn, "SELECT LAST_INSERT_ID();", new ScalarHandler<>(1)))
          .longValue();
    } catch (final SQLException ex) {
      logger.error("can not get last insertion ID");
      throw ex;
    }
    return num;
  }

  /**
   *
   * @param querySql
   * @param resultHandler
   * @param params
   * @param <T>
   * @return
   * @throws SQLException
   */
  public <T> T query(final String querySql, final ResultSetHandler<T> resultHandler,
      final Object... params)
      throws SQLException {
    try {
      return this.queryRunner.query(this.conn, querySql, resultHandler, params);
    } catch (final SQLException ex) {
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }

  /**
   *
   * @param querySql
   * @param resultHandler
   * @param <T>
   * @return
   * @throws SQLException
   */
  public <T> T query(final String querySql, final ResultSetHandler<T> resultHandler) throws SQLException {
      return query(querySql, resultHandler, (Object[])null);
  }

  /**
   *
   * @param updateClause
   * @param params
   * @return
   * @throws SQLException
   */
  public int update(final String updateClause, final Object... params) throws SQLException {
    try {
      return this.queryRunner.update(this.conn, updateClause, params);
    } catch (final SQLException ex) {
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }

  /**
   * @return the JDBC connection associated with this operator.
   */
  public Connection getConnection() {
    return this.conn;
  }
}

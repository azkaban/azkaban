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

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.SQLException;
import javax.inject.Inject;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This interface is to define Base Data Access Object contract for Azkaban. All azkaban DB related
 * operations must be performed upon this interface. AZ DB operators all leverages QueryRunner
 * interface.
 *
 * @see org.apache.commons.dbutils.QueryRunner
 */
public class DatabaseOperator {

  private static final Logger LOG = LoggerFactory.getLogger(DatabaseOperator.class);

  private final QueryRunner queryRunner;

  @Inject
  private DBMetrics dbMetrics;

  /**
   * Note: this queryRunner should include a concrete {@link AzkabanDataSource} inside.
   */
  @Inject
  public DatabaseOperator(final QueryRunner queryRunner) {
    requireNonNull(queryRunner.getDataSource(), "data source must not be null.");
    this.queryRunner = queryRunner;
  }

  /**
   * Executes the given Azkaban related SELECT SQL operations. it will call
   * {@link AzkabanDataSource#getConnection()} inside queryrunner.query.
   *
   * @param resultHandler The handler used to create the result object
   * @param params Initialize the PreparedStatement's IN parameters
   * @param <T> The type of object that the qeury handler returns
   * @return The object returned by the handler.
   */
  public <T> T query(final String baseQuery, final ResultSetHandler<T> resultHandler,
      final Object... params)
      throws SQLException {
    try {
      return this.queryRunner.query(baseQuery, resultHandler, params);
    } catch (final SQLException ex) {
      // todo kunkun-tang: Retry logics should be implemented here.
      LOG.error("query failed", ex);
      if (this.dbMetrics != null) {
        this.dbMetrics.markDBFailQuery();
      }
      throw ex;
    }
  }

  /**
   * Provide a way to allow users define custom SQL operations without relying on fixed SQL
   * interface. The common use case is to group a sequence of SQL operations without commit every
   * time.
   *
   * @param operations A sequence of DB operations
   * @param <T> The type of object that the operations returns. Note that T could be null
   * @return T The object returned by the SQL statement, expected by the caller
   */
  public <T> T transaction(final SQLTransaction<T> operations) throws SQLException {
    Connection conn = null;
    try {
      conn = this.queryRunner.getDataSource().getConnection();
      conn.setAutoCommit(false);
      final DatabaseTransOperator transOperator = new DatabaseTransOperator(this.queryRunner,
          conn);
      final T res = operations.execute(transOperator);
      conn.commit();
      return res;
    } catch (final SQLException ex) {
      // todo kunkun-tang: Retry logics should be implemented here.
      LOG.error("transaction failed", ex);
      if (this.dbMetrics != null) {
        this.dbMetrics.markDBFailTransaction();
      }
      throw ex;
    } finally {
      DbUtils.closeQuietly(conn);
    }
  }

  /**
   * Executes the given AZ related INSERT, UPDATE, or DELETE SQL statement.
   * it will call {@link AzkabanDataSource#getConnection()} inside
   * queryrunner.update.
   *
   * @param updateClause sql statements to execute
   * @param params Initialize the PreparedStatement's IN parameters
   * @return The number of rows updated.
   */
  public int update(final String updateClause, final Object... params) throws SQLException {
    try {
      return this.queryRunner.update(updateClause, params);
    } catch (final SQLException ex) {
      // todo kunkun-tang: Retry logics should be implemented here.
      LOG.error("update failed", ex);
      if (this.dbMetrics != null) {
        this.dbMetrics.markDBFailUpdate();
      }
      throw ex;
    }
  }

  /**
   * @return datasource wrapped in the database operator.
   */
  public AzkabanDataSource getDataSource() {
    return (AzkabanDataSource) this.queryRunner.getDataSource();
  }
}

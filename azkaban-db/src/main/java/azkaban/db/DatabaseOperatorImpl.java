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

import java.sql.Connection;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import com.google.inject.Inject;

import static java.util.Objects.*;

/**
 * Implement AZ DB related operations. This class is thread safe.
 */
public class DatabaseOperatorImpl implements DatabaseOperator {

  private static final Logger logger = Logger.getLogger(DatabaseOperatorImpl.class);

  private final QueryRunner queryRunner;

  /**
   * Note: this queryRunner should include a concrete {@link AzkabanDataSource} inside.
   *
   * @param queryRunner
   */
  @Inject
  public DatabaseOperatorImpl(QueryRunner queryRunner){
    requireNonNull(queryRunner.getDataSource(), "data source must not be null.");
    this.queryRunner = queryRunner;
  }

  /**
   * query method Implementation. it will call {@link AzkabanDataSource#getConnection()} inside queryrunner.query.
   */
  @Override
  public <T> T query(String baseQuery, ResultSetHandler<T> resultHandler, Object...params) throws SQLException {
    try{
      return queryRunner.query(baseQuery, resultHandler, params);
    } catch (SQLException ex){
      // todo kunkun-tang: Retry logics should be implemented here.
      logger.error("query failed", ex);
      throw ex;
    }
  }

  /**
   * transaction method Implementation.
   *
   */
  @Override
  public <T> T transaction(SQLTransaction<T> operations) throws SQLException {
    Connection conn = null;
    try{
      conn = queryRunner.getDataSource().getConnection();
      conn.setAutoCommit(false);
      DatabaseTransOperator transOperator = new DatabaseTransOperatorImpl(queryRunner, conn);
      T res = operations.execute(transOperator);
      conn.commit();
      return res;
    } catch (SQLException ex) {
      // todo kunkun-tang: Retry logics should be implemented here.
      logger.error("transaction failed", ex);
      throw ex;
    } finally {
      DbUtils.closeQuietly(conn);
    }
  }

  /**
   * update implementation. it will call {@link AzkabanDataSource#getConnection()} inside queryrunner.update.
   *
   * @param updateClause sql statements to execute
   * @param params Initialize the PreparedStatement's IN parameters
   * @return the number of rows being affected by update
   * @throws SQLException
   */
  @Override
  public int update(String updateClause, Object...params) throws SQLException {
    try{
      return queryRunner.update(updateClause, params);
    } catch (SQLException ex){
      // todo kunkun-tang: Retry logics should be implemented here.
      logger.error("update failed", ex);
      throw ex;
    }
  }
}

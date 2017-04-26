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
import java.sql.SQLException;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;


/**
 * This class should be only used inside {@link DatabaseOperatorImpl}, then we remove public.
 */
class DatabaseTransOperatorImpl implements DatabaseTransOperator {

  private final Connection conn;
  private final QueryRunner queryRunner;

  public DatabaseTransOperatorImpl(QueryRunner queryRunner, Connection conn) {
    this.conn = conn;
    this.queryRunner= queryRunner;
  }

  @Override
  public <T> T query(String querySql, ResultSetHandler<T> resultHandler, Object... params) throws SQLException {
    try{
      return queryRunner.query(conn, querySql, resultHandler, params);
    } catch (SQLException ex){
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }

  @Override
  public int update(String updateClause, Object... params) throws SQLException {
    try{
      return queryRunner.update(conn, updateClause, params);
    } catch (SQLException ex){
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }
}

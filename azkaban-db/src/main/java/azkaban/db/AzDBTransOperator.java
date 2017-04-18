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
 * This interface is designed as an supplement of {@link AzDBOperator}, which do commit at the end of every query. Given
 * this interface, users/callers (implementation code) should decide where to {@link Connection#commit()}
 * based on their requirements.
 *
 * @see org.apache.commons.dbutils.QueryRunner
 */
public interface AzDBTransOperator {

  /**
   *
   * @param querySql
   * @param resultHandler
   * @param params
   * @param <T>
   * @return
   * @throws SQLException
   */
  <T> T query(String querySql, ResultSetHandler<T> resultHandler,
      Object... params) throws SQLException;

  /**
   *
   * @param updateClause
   * @param params
   * @return
   * @throws SQLException
   */
  int update(String updateClause, Object... params) throws SQLException;
}

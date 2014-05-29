/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import azkaban.utils.Props;

public abstract class AbstractJdbcLoader {
  /**
   * Used for when we store text data. Plain uses UTF8 encoding.
   */
  public static enum EncodingType {
    PLAIN(1), GZIP(2);

    private int numVal;

    EncodingType(int numVal) {
      this.numVal = numVal;
    }

    public int getNumVal() {
      return numVal;
    }

    public static EncodingType fromInteger(int x) {
      switch (x) {
      case 1:
        return PLAIN;
      case 2:
        return GZIP;
      default:
        return PLAIN;
      }
    }
  }

  private AzkabanDataSource dataSource;

  public AbstractJdbcLoader(Props props) {
    dataSource = DataSourceUtils.getDataSource(props);
  }

  protected Connection getDBConnection(boolean autoCommit) throws IOException {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      connection.setAutoCommit(autoCommit);
    } catch (Exception e) {
      DbUtils.closeQuietly(connection);
      throw new IOException("Error getting DB connection.", e);
    }

    return connection;
  }

  protected QueryRunner createQueryRunner() {
    return new QueryRunner(dataSource);
  }

  protected boolean allowsOnDuplicateKey() {
    return dataSource.allowsOnDuplicateKey();
  }

  public static class IntHandler implements ResultSetHandler<Integer> {
    @Override
    public Integer handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return 0;
      }

      return rs.getInt(1);
    }
  }

  public static class SingleStringHandler implements ResultSetHandler<String> {
    @Override
    public String handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }

      return rs.getString(1);
    }
  }

  public static class IntListHandler implements
      ResultSetHandler<ArrayList<Integer>> {
    @Override
    public ArrayList<Integer> handle(ResultSet rs) throws SQLException {
      ArrayList<Integer> results = new ArrayList<Integer>();
      while (rs.next()) {
        results.add(rs.getInt(1));
      }

      return results;
    }
  }

  public static class StringListHandler implements
      ResultSetHandler<ArrayList<String>> {
    @Override
    public ArrayList<String> handle(ResultSet rs) throws SQLException {
      ArrayList<String> results = new ArrayList<String>();
      while (rs.next()) {
        results.add(rs.getString(1));
      }

      return results;
    }
  }
}

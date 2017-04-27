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

import azkaban.metrics.CommonMetrics;
import azkaban.utils.Props;

import java.io.IOException;
import java.sql.Connection;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;


public abstract class AbstractJdbcLoader {
  /**
   * Used for when we store text data. Plain uses UTF8 encoding.
   */
  public enum EncodingType {
    PLAIN(1), GZIP(2);

    private final int numVal;

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
    CommonMetrics.INSTANCE.markDBConnection();
    long startMs = System.currentTimeMillis();
    try {
      connection = dataSource.getConnection();
      connection.setAutoCommit(autoCommit);
    } catch (Exception e) {
      DbUtils.closeQuietly(connection);
      throw new IOException("Error getting DB connection.", e);
    }
    CommonMetrics.INSTANCE.setDBConnectionTime(System.currentTimeMillis() - startMs);
    return connection;
  }

  protected QueryRunner createQueryRunner() {
    return new QueryRunner(dataSource);
  }

  protected boolean allowsOnDuplicateKey() {
    return dataSource.allowsOnDuplicateKey();
  }
}

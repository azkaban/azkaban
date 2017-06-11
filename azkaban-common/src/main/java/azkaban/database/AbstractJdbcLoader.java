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

  private final AzkabanDataSource dataSource;
  private final CommonMetrics commonMetrics;

  public AbstractJdbcLoader(final Props props, final CommonMetrics commonMetrics) {
    this.dataSource = DataSourceUtils.getDataSource(props);
    this.commonMetrics = commonMetrics;
  }

  protected Connection getDBConnection(final boolean autoCommit) throws IOException {
    Connection connection = null;
    this.commonMetrics.markDBConnection();
    final long startMs = System.currentTimeMillis();
    try {
      connection = this.dataSource.getConnection();
      connection.setAutoCommit(autoCommit);
    } catch (final Exception e) {
      DbUtils.closeQuietly(connection);
      throw new IOException("Error getting DB connection.", e);
    }
    this.commonMetrics.setDBConnectionTime(System.currentTimeMillis() - startMs);
    return connection;
  }

  protected QueryRunner createQueryRunner() {
    return new QueryRunner(this.dataSource);
  }

  protected boolean allowsOnDuplicateKey() {
    return this.dataSource.allowsOnDuplicateKey();
  }

  /**
   * Used for when we store text data. Plain uses UTF8 encoding.
   */
  public enum EncodingType {
    PLAIN(1), GZIP(2);

    private final int numVal;

    EncodingType(final int numVal) {
      this.numVal = numVal;
    }

    public static EncodingType fromInteger(final int x) {
      switch (x) {
        case 1:
          return PLAIN;
        case 2:
          return GZIP;
        default:
          return PLAIN;
      }
    }

    public int getNumVal() {
      return this.numVal;
    }
  }
}

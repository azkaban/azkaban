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

package azkaban.database;

import java.sql.Connection;
import org.apache.commons.dbutils.DbUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class AzkabanConnectionPoolTest {

  AzkabanDataSource h2DataSource;
  Connection connection;

  @Before
  public void setup() throws Exception {
    this.h2DataSource = new EmbeddedH2BasicDataSource();
    this.connection = this.h2DataSource.getConnection();
  }

  @Test
  public void testConnectionDefaultAutoCommit() throws Exception {
    Assert.assertEquals(this.connection.getAutoCommit(), true);
    DbUtils.closeQuietly(this.connection);
  }

  @Test
  public void testConnectionDisableAutoCommit() throws Exception {
    this.connection.setAutoCommit(false);
    Assert.assertEquals(this.connection.getAutoCommit(), false);
    DbUtils.closeQuietly(this.connection);
  }

  @Test
  public void testGetNewConnectionBeforeClose() throws Exception {
    this.connection.setAutoCommit(false);

    /**
     * {@link AzkabanDataSource#getConnection} fetches a new connection object other than one in the above, if we don't close.
     */
    Assert.assertEquals(this.h2DataSource.getConnection().getAutoCommit(), true);
    DbUtils.closeQuietly(this.connection);
  }

  @Test
  public void testGetNewConnectionAfterClose() throws Exception {
    this.connection.setAutoCommit(false);

    /**
     * See {@link org.apache.commons.dbcp2.PoolableConnectionFactory#passivateObject}.
     * If the connection disables auto commit, when we close it, connection will be reset enabling auto commit,
     * and returned to connection pool.
     */
    DbUtils.closeQuietly(this.connection);
    final Connection newConnection = this.h2DataSource.getConnection();
    Assert.assertEquals(newConnection.getAutoCommit(), true);

    DbUtils.closeQuietly(newConnection);
  }

  public static class EmbeddedH2BasicDataSource extends AzkabanDataSource {

    public EmbeddedH2BasicDataSource() {
      super();
      final String url = "jdbc:h2:mem:test;IGNORECASE=TRUE";
      setDriverClassName("org.h2.Driver");
      setUrl(url);
    }

    @Override
    public boolean allowsOnDuplicateKey() {
      return false;
    }

    @Override
    public String getDBType() {
      return "h2-in-memory";
    }
  }
}

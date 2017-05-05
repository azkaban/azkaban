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
import java.sql.SQLException;
import org.apache.commons.dbutils.DbUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class AzkabanConnectionPoolTest{

  public static class EmbeddedH2BasicDataSource extends AzkabanDataSource {

    public EmbeddedH2BasicDataSource() {
      super();
      String url = "jdbc:h2:mem:test";
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

  AzkabanDataSource h2DataSource;
  Connection connection;

  @Before
  public void setup() throws Exception{
    h2DataSource = new EmbeddedH2BasicDataSource();
    connection = h2DataSource.getConnection();
  }


  @Test
  public void testConnectionDefaultAutoCommit() throws Exception {
    Assert.assertEquals(connection.getAutoCommit(), true);
    DbUtils.closeQuietly(connection);
  }

  @Test
  public void testConnectionDisableAutoCommit() throws Exception {
    connection.setAutoCommit(false);
    Assert.assertEquals(connection.getAutoCommit(), false);
    DbUtils.closeQuietly(connection);
  }

  @Test
  public void testGetNewConnectionBeforeClose() throws Exception {
    connection.setAutoCommit(false);

    /**
     * {@link AzkabanDataSource#getConnection} fetches a new connection object other than one in the above, if we don't close.
     */
    Assert.assertEquals(h2DataSource.getConnection().getAutoCommit(), true);
    DbUtils.closeQuietly(connection);
  }

  @Test
  public void testGetNewConnectionAfterClose() throws Exception {
    connection.setAutoCommit(false);

    /**
     * See {@link org.apache.commons.dbcp2.PoolableConnectionFactory#passivateObject}.
     * If the connection disables auto commit, when we close it, connection will be reset enabling auto commit,
     * and returned to connection pool.
     */
    DbUtils.closeQuietly(connection);
    Connection newConnection = h2DataSource.getConnection();
    Assert.assertEquals(newConnection.getAutoCommit(), true);

    DbUtils.closeQuietly(newConnection);
  }
}

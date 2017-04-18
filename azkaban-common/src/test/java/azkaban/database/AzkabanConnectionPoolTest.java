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

  public static class EmbeddedDerbyBasicDataSource extends AzkabanDataSource {

    private EmbeddedDerbyBasicDataSource() {
      super();
      String url = "jdbc:derby:memory:MyDatabase;create=true";
      setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
      setUrl(url);
    }

    @Override
    public boolean allowsOnDuplicateKey() {
      return false;
    }

    @Override
    public String getDBType() {
      return "Derby";
    }
  }

  AzkabanDataSource derbyDataSource;

  @Before
  public void setup() throws Exception{
    derbyDataSource = new EmbeddedDerbyBasicDataSource();
  }

  @Test
  public void testCoonectionPoolAutoCommitRest() throws Exception {
    Connection connection = derbyDataSource.getConnection();
    Assert.assertEquals(connection.getAutoCommit(), true);

    connection.setAutoCommit(false);
    Assert.assertEquals(connection.getAutoCommit(), false);

    /**
     * {@link AzkabanDataSource#getConnection} fetches a different connection object other than one in the above.
     */
    Assert.assertEquals(derbyDataSource.getConnection().getAutoCommit(), true);

    /**
     * See {@link org.apache.commons.dbcp2.PoolableConnectionFactory#passivateObject}.
     * If the connection disabled auto commit, when we close it, connection will be reset back to enable auto commit,
     * and returned to connection pool.
     */
    DbUtils.closeQuietly(connection);

    Connection newConnection = derbyDataSource.getConnection();
    Assert.assertEquals(newConnection.getAutoCommit(), true);

    // old connection object points to null.
    Assert.assertNotEquals(newConnection, connection);
    DbUtils.closeQuietly(connection);
  }
}

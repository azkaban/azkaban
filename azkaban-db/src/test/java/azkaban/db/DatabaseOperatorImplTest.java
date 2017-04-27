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
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class DatabaseOperatorImplTest {

  private AzkabanDataSource datasource = new AzDBTestUtility.EmbeddedH2BasicDataSource();

  private DatabaseOperator dbOperator;
  private QueryRunner queryRunner;
  private Connection conn;

  private ResultSetHandler<Integer> handler = rs -> {
    if (!rs.next()) {
      return 0;
    }
    return rs.getInt(1);
  };

  private static final List<Integer> list = new ArrayList<>();

  private static int index_1 = 3;
  private static int index_2 = 15;

  @Before
  public void setUp() throws Exception {
    queryRunner = mock(QueryRunner.class);

    conn = datasource.getConnection();
    DataSource mockDataSource = mock(datasource.getClass());

    when(queryRunner.getDataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(conn);

    this.dbOperator = new DatabaseOperatorImpl(queryRunner);

    list.add(index_1);
    list.add(index_2);

    // valid query returns correct value
    when(queryRunner.query("select * from blah where ? = ?", handler, "id", 2)).thenReturn(index_2);

    // If select an non-existing entry, handler returns 0.
    when(queryRunner.query("select * from blah where ? = ?", handler, "id", 3)).thenReturn(0);

    //If typos, throw Exceptions.
    doThrow(SQLException.class).when(queryRunner).query("sele * from blah where ? = ?", handler, "id", 2);

    doAnswer(invocation -> {
      index_1 = 26;
      return 1;
    }).when(queryRunner).update("update blah set ? = ?", "1", 26);
  }

  @Test
  public void testValidQuery() throws Exception {
    int res = dbOperator.query("select * from blah where ? = ?", handler, "id", 2);
    Assert.assertEquals(15, res);
    verify(queryRunner).query("select * from blah where ? = ?", handler, "id", 2);
  }

  @Test
  public void testInvalidQuery() throws Exception {
    int res = dbOperator.query("select * from blah where ? = ?", handler, "id", 3);
    Assert.assertEquals(0, res);
  }

  @Test(expected = SQLException.class)
  public void testTypoSqlStatement() throws Exception {
    System.out.println("testTypoSqlStatement");
    dbOperator.query("sele * from blah where ? = ?", handler, "id", 2);
  }

  @Test
  public void testTransaction() throws Exception {
    when(queryRunner.update(conn, "update blah set ? = ?", "1", 26)).thenReturn(1);
    when(queryRunner.query(conn, "select * from blah where ? = ?", handler, "id", 1)).thenReturn(26);

    SQLTransaction<Integer> transaction = transOperator -> {
      transOperator.update("update blah set ? = ?", "1", 26);
      return transOperator.query("select * from blah where ? = ?", handler, "id", 1);
    };

    int res = dbOperator.transaction(transaction);
    Assert.assertEquals(26, res);
  }

  @Test
  public void testValidUpdate() throws Exception {
    int res = dbOperator.update("update blah set ? = ?", "1", 26);

    // 1 row is affected
    Assert.assertEquals(1, res);
    Assert.assertEquals(26, index_1);
    verify(queryRunner).update("update blah set ? = ?", "1", 26);
  }

  @Test
  public void testInvalidUpdate() throws Exception {
    int res = dbOperator.update("update blah set ? = ?", "3", 26);

    // 0 row is affected
    Assert.assertEquals(0, res);
  }
}

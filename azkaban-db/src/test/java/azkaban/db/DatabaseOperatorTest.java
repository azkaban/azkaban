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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class DatabaseOperatorTest {

  private static final List<Integer> list = new ArrayList<>();
  private static final int index_2 = 15;
  private static int index_1 = 3;
  private final AzkabanDataSource datasource = new AzDBTestUtility.EmbeddedH2BasicDataSource();
  private final ResultSetHandler<Integer> handler = rs -> {
    if (!rs.next()) {
      return 0;
    }
    return rs.getInt(1);
  };
  private DatabaseOperator dbOperator;
  private QueryRunner queryRunner;
  private Connection conn;

  private static String BATCH_COMMAND = "INSERT INTO tb (rampId, flowId, treatment, timestamp) VALUES(?,?,?,?)";
  private static Object[][] BATCH_PARAMETERS = {
      {"rampId1", "flowId1", "S", 1566494649000L},
      {"rampId2", "flowId2", "U", 1566494650000L}
  };
  private static int[] BATCH_COMMAND_RESULT = {1, 1};

  @Before
  public void setUp() throws Exception {
    this.queryRunner = mock(QueryRunner.class);

    this.conn = this.datasource.getConnection();
    final DataSource mockDataSource = mock(this.datasource.getClass());

    when(this.queryRunner.getDataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(this.conn);

    this.dbOperator = new DatabaseOperator(this.queryRunner);

    list.add(index_1);
    list.add(index_2);

    // valid query returns correct value
    when(this.queryRunner.query("select * from blah where ? = ?", this.handler, "id", 2))
        .thenReturn(index_2);

    // If select an non-existing entry, handler returns 0.
    when(this.queryRunner.query("select * from blah where ? = ?", this.handler, "id", 3))
        .thenReturn(0);


    when(this.queryRunner.batch(BATCH_COMMAND, BATCH_PARAMETERS)).thenReturn(BATCH_COMMAND_RESULT);

    //If typos, throw Exceptions.
    doThrow(SQLException.class).when(this.queryRunner)
        .query("sele * from blah where ? = ?", this.handler, "id", 2);

    doAnswer(invocation -> {
      index_1 = 26;
      return 1;
    }).when(this.queryRunner).update("update blah set ? = ?", "1", 26);

  }

  @Test
  public void testValidQuery() throws Exception {
    final int res = this.dbOperator.query("select * from blah where ? = ?", this.handler, "id", 2);
    Assert.assertEquals(15, res);
    verify(this.queryRunner).query("select * from blah where ? = ?", this.handler, "id", 2);
  }

  @Test
  public void testInvalidQuery() throws Exception {
    final int res = this.dbOperator.query("select * from blah where ? = ?", this.handler, "id", 3);
    Assert.assertEquals(0, res);
  }

  @Test(expected = SQLException.class)
  public void testTypoSqlStatement() throws Exception {
    System.out.println("testTypoSqlStatement");
    this.dbOperator.query("sele * from blah where ? = ?", this.handler, "id", 2);
  }

  @Test
  public void testTransaction() throws Exception {
    when(this.queryRunner.update(this.conn, "update blah set ? = ?", "1", 26)).thenReturn(1);
    when(this.queryRunner.query(this.conn, "select * from blah where ? = ?", this.handler, "id", 1))
        .thenReturn(26);

    final SQLTransaction<Integer> transaction = transOperator -> {
      transOperator.update("update blah set ? = ?", "1", 26);
      return transOperator.query("select * from blah where ? = ?", this.handler, "id", 1);
    };

    final int res = this.dbOperator.transaction(transaction);
    Assert.assertEquals(26, res);
  }

  @Test
  public void testValidUpdate() throws Exception {
    final int res = this.dbOperator.update("update blah set ? = ?", "1", 26);

    // 1 row is affected
    Assert.assertEquals(1, res);
    Assert.assertEquals(26, index_1);
    verify(this.queryRunner).update("update blah set ? = ?", "1", 26);
  }

  @Test
  public void testInvalidUpdate() throws Exception {
    final int res = this.dbOperator.update("update blah set ? = ?", "3", 26);

    // 0 row is affected
    Assert.assertEquals(0, res);
  }

  @Test
  public void testBatch() throws SQLException {
    int[] res = this.dbOperator.batch(BATCH_COMMAND, BATCH_PARAMETERS);
    Assert.assertEquals(BATCH_COMMAND_RESULT, res);
  }
}

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

package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.db.DatabaseOperator;
import azkaban.test.Utils;
import azkaban.utils.TestUtils;
import java.sql.SQLException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class NumExecutionsDaoTest {

  private static DatabaseOperator dbOperator;
  private NumExecutionsDao numExecutionsDao;
  private ExecutionFlowDao executionFlowDao;

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = Utils.initTestDB();
  }

  @AfterClass
  public static void destroyDB() throws Exception {
    try {
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() {
    MysqlNamedLock mysqlNamedLock = Mockito.mock(MysqlNamedLock.class);
    this.executionFlowDao = new ExecutionFlowDao(dbOperator, mysqlNamedLock);
    this.numExecutionsDao = new NumExecutionsDao(dbOperator);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("delete from execution_flows");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testFetchNumExecutableFlows() throws Exception {
    final ExecutableFlow flow1 = TestUtils.createTestExecutableFlow("exectest1", "exec1");
    flow1.setStatus(Status.READY);
    this.executionFlowDao.uploadExecutableFlow(flow1);

    final ExecutableFlow flow2 = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    flow2.setStatus(Status.RUNNING);
    this.executionFlowDao.uploadExecutableFlow(flow2);

    final ExecutableFlow flow2b = TestUtils.createTestExecutableFlow("exectest1", "exec2");
    flow2b.setStatus(Status.FAILED);
    this.executionFlowDao.uploadExecutableFlow(flow2b);

    final int count = this.numExecutionsDao.fetchNumExecutableFlows();
    assertThat(count).isEqualTo(3);

    final int flow2Count = this.numExecutionsDao
        .fetchNumExecutableFlows(1, "derived-member-data-2");
    assertThat(flow2Count).isEqualTo(2);
  }

  @Test
  public void testFetchNumExecutableNodes() throws Exception {
    // This test will be filled up after execution_jobs test completes.
  }
}

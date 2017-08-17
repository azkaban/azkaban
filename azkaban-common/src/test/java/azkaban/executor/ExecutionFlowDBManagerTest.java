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
import java.util.HashSet;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutionFlowDBManagerTest {

  private static DatabaseOperator dbOperator;
  private ExecutionFlowDBManager executionFlowDBManager;

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = Utils.initTestDB();
  }

  @AfterClass
  public static void destroyDB() throws Exception {
    try {
      dbOperator.update("DROP ALL OBJECTS");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() {
    this.executionFlowDBManager = new ExecutionFlowDBManager(dbOperator);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("DELETE FROM execution_flows");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  private ExecutableFlow createTestFlow() throws Exception {
    return TestUtils.createExecutableFlow("exectest1", "exec1");
  }

  @Test
  public void testUploadAndFetchExecutionFlows() throws Exception {

    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDBManager.uploadExecutableFlow(flow);

    final ExecutableFlow fetchFlow =
        this.executionFlowDBManager.fetchExecutableFlow(flow.getExecutionId());

    assertThat(flow).isNotSameAs(fetchFlow);
    assertTwoFlowSame(flow, fetchFlow);
  }


  @Test
  public void testUpdateExecutableFlow() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDBManager.uploadExecutableFlow(flow);

    final ExecutableFlow fetchFlow =
        this.executionFlowDBManager.fetchExecutableFlow(flow.getExecutionId());

    fetchFlow.setEndTime(System.currentTimeMillis());
    fetchFlow.setStatus(Status.SUCCEEDED);
    this.executionFlowDBManager.updateExecutableFlow(fetchFlow);
    final ExecutableFlow fetchFlow2 =
        this.executionFlowDBManager.fetchExecutableFlow(flow.getExecutionId());

    assertTwoFlowSame(fetchFlow, fetchFlow2);
  }

  @Test
  public void fetchFlowHistory() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    this.executionFlowDBManager.uploadExecutableFlow(flow);
    final List<ExecutableFlow> flowList1 = this.executionFlowDBManager.fetchFlowHistory(0,2 );
    assertThat(flowList1.size()).isEqualTo(1);

    final List<ExecutableFlow> flowList2 = this.executionFlowDBManager
        .fetchFlowHistory(flow.getProjectId(), flow.getId(),0,2 );
    assertThat(flowList2.size()).isEqualTo(1);

    final ExecutableFlow fetchFlow =
        this.executionFlowDBManager.fetchExecutableFlow(flow.getExecutionId());
    assertTwoFlowSame(flowList1.get(0), flowList2.get(0));
    assertTwoFlowSame(flowList1.get(0), fetchFlow);
  }

  private void assertTwoFlowSame(final ExecutableFlow flow1, final ExecutableFlow flow2) {
    assertThat(flow1.getExecutionId()).isEqualTo(flow2.getExecutionId());
    assertThat(flow1.getStatus()).isEqualTo(flow2.getStatus());
    assertThat(flow1.getEndTime()).isEqualTo(flow2.getEndTime());
    assertThat(flow1.getStartTime()).isEqualTo(flow2.getStartTime());
    assertThat(flow1.getSubmitTime()).isEqualTo(flow2.getStartTime());
    assertThat(flow1.getFlowId()).isEqualTo(flow2.getFlowId());
    assertThat(flow1.getProjectId()).isEqualTo(flow2.getProjectId());
    assertThat(flow1.getVersion()).isEqualTo(flow2.getVersion());
    assertThat(flow1.getExecutionOptions().getFailureAction())
        .isEqualTo(flow2.getExecutionOptions().getFailureAction());
    assertThat(new HashSet<>(flow1.getEndNodes())).isEqualTo(new HashSet<>(flow2.getEndNodes()));
  }
}

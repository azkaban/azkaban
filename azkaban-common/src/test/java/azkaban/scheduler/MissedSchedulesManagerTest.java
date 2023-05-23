/*
 * Copyright 2022 LinkedIn Corp.
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

package azkaban.scheduler;

import azkaban.Constants;
import azkaban.executor.ExecutionOptions;
import azkaban.flow.Flow;
import azkaban.metrics.MetricsManager;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import com.codahale.metrics.Counter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class MissedSchedulesManagerTest {
  MissedSchedulesManager _missedSchedulesManager;
  ProjectManager projectManager;
  MetricsManager metricsManager;
  Counter fakeCounter;
  Emailer emailer;

  @Before
  public void setup() {
    this.projectManager = mock(ProjectManager.class);
    this.emailer = mock(Emailer.class);
    this.metricsManager = mock(MetricsManager.class);
    this.fakeCounter = new Counter();
    final Props azkProps = new Props();
    azkProps.put(Constants.MISSED_SCHEDULE_MANAGER_ENABLED, "true");
    when(this.metricsManager.addCounter(any())).thenReturn(this.fakeCounter);

    this._missedSchedulesManager = new MissedSchedulesManager(azkProps, this.projectManager, this.emailer,
        this.metricsManager);
    this._missedSchedulesManager.start();
  }

  @After
  public void cleanup() throws InterruptedException {
    this._missedSchedulesManager.stop();
  }

  @Test
  public void testAddSchedule() {
    final List<Long> timestamps = new ArrayList<>();
    final long curTime = System.currentTimeMillis();
    timestamps.add(curTime);
    final ExecuteFlowAction executeFlowAction = mock(ExecuteFlowAction.class);
    final String projectName = "testProjectName";
    final Project project = new Project(1111, projectName);
    final String flowName = "testFlow";
    final Flow flow = new Flow(flowName);
    final String emailAddress = "azkaban_dev@linkedin.com";
    flow.addFailureEmails(Collections.singletonList(emailAddress));
    final Map<String, Flow> flowMap = new HashMap<>();
    flowMap.put(flowName, flow);
    project.setFlows(flowMap);
    when(executeFlowAction.getFlowName()).thenReturn(flowName);
    when(executeFlowAction.getProjectName()).thenReturn(projectName);
    when(this.projectManager.getProject(projectName)).thenReturn(project);
    when(executeFlowAction.getExecutionOptions()).thenReturn(new ExecutionOptions());
    // verify task is added into queue
    Assertions.assertThat(this._missedSchedulesManager.addMissedSchedule(timestamps, executeFlowAction, false)).isTrue();

  }
}

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
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
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
  Emailer emailer;

  @Before
  public void setup() {
    this.projectManager = mock(ProjectManager.class);
    this.emailer = mock(Emailer.class);
    Props azkProps = new Props();
    azkProps.put(Constants.MISSED_SCHEDULE_MANAGER_ENABLED, "true");
    _missedSchedulesManager = new MissedSchedulesManager(azkProps, projectManager, emailer);
  }

  @After
  public void cleanup() throws InterruptedException {
    _missedSchedulesManager.stop();
  }

  @Test
  public void testAddSchedule() {
    List<Long> timestamps = new ArrayList<>();
    long curTime = System.currentTimeMillis();
    timestamps.add(curTime);
    ExecuteFlowAction executeFlowAction = mock(ExecuteFlowAction.class);
    String projectName = "testProjectName";
    Project project = new Project(1111, projectName);
    String flowName = "testFlow";
    Flow flow = new Flow(flowName);
    String emailAddress = "azkaban_dev@linkedin.com";
    flow.addFailureEmails(Collections.singletonList(emailAddress));
    Map<String, Flow> flowMap = new HashMap<>();
    flowMap.put(flowName, flow);
    project.setFlows(flowMap);
    when(executeFlowAction.getFlowName()).thenReturn(flowName);
    when(executeFlowAction.getProjectName()).thenReturn(projectName);
    when(projectManager.getProject(projectName)).thenReturn(project);
    // verify task is added into queue
    _missedSchedulesManager.addMissedSchedule(timestamps, executeFlowAction, false);
    Assertions.assertThat(_missedSchedulesManager.isTaskQueueEmpty()).isFalse();

    // verify email body contains project, flow, and date information
    MissedSchedulesManager.MissedScheduleTaskQueueNode node = _missedSchedulesManager.peekFirstTask();
    Assertions.assertThat(node.getEmailMessage()).contains(projectName, flowName, new Date(curTime).toString());
  }
}

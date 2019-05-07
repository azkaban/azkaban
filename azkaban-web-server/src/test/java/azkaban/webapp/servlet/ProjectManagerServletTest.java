/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.webapp.servlet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.project.DirectoryYamlFlowLoader;
import azkaban.project.Project;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class ProjectManagerServletTest {

  private final ScheduleManager scheduleManager = mock(ScheduleManager.class);
  private final ProjectManagerServlet projectManagerServlet = mock(ProjectManagerServlet.class);

  @Test
  public void testRemoveScheduleOfDeletedFlows() throws Exception {
    final Project project = new Project(1, "myTestProject");
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(project, ExecutionsTestUtil.getFlowDir("multipleflowyamltest"));
    project.setFlows(loader.getFlowMap());
    Assert.assertEquals(2, project.getFlows().size());
    final Set<String> flowNameList =
        project.getFlows().stream().map(f -> f.getId()).collect(Collectors.toSet());
    Assert.assertTrue(flowNameList.containsAll(Arrays.asList("basic_flow", "basic_flow2")));

    final List<Schedule> schedules = new ArrayList<>();
    final long now = System.currentTimeMillis();
    final Schedule sched1 = new Schedule(1, 1, "myTestProject", "basic_flow_renamed", "ready", now,
        now, null, null, now, now, now, "testUser1", null, null);
    schedules.add(sched1);
    final Schedule sched2 = new Schedule(2, 1, "myTestProject", "basic_flow2", "ready", now + 20,
        now + 20, null, null, now + 20, now + 20, now + 20, "testUser1", null, null);
    schedules.add(sched2);
    final Schedule sched3 = new Schedule(3, 3, "anotherTestProject", "anotherFlow", "ready",
        now + 30, now + 30, null, null, now + 30, now + 30, now + 30, "testUser3", null, null);
    schedules.add(sched3);

    when(this.scheduleManager.getSchedules()).thenReturn(new ArrayList<>(schedules));
    doAnswer(invocation -> schedules.remove(invocation.getArguments()[0]))
        .when(this.scheduleManager).removeSchedule(any(Schedule.class));
    this.projectManagerServlet
        .removeScheduleOfDeletedFlows(project, this.scheduleManager, schedule -> {
        });
    Assert.assertEquals(2, schedules.size());
    Assert.assertTrue(schedules.containsAll(Arrays.asList(sched2, sched3)));
  }

}

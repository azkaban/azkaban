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

package azkaban.flowtrigger.quartz;

import azkaban.flow.FlowUtils;
import azkaban.flowtrigger.FlowTriggerService;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.scheduler.AbstractQuartzJob;
import javax.inject.Inject;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;


public class FlowTriggerQuartzJob extends AbstractQuartzJob {

  public static final String SUBMIT_USER = "SUBMIT_USER";
  public static final String PROJECT = "PROJECT";
  public static final String FLOW_TRIGGER = "FLOW_TRIGGER";
  public static final String FLOW_ID = "FLOW_ID";
  public static final String FLOW_VERSION = "FLOW_VERSION";

  private final FlowTriggerService triggerService;

  @Inject
  public FlowTriggerQuartzJob(final FlowTriggerService service) {
    this.triggerService = service;
  }

  @Override
  public void execute(final JobExecutionContext context) {
    final JobDataMap data = context.getMergedJobDataMap();
    final String projectJson = data.getString(PROJECT);
    final Project project = FlowUtils.toProject(projectJson);

    final String flowId = data.getString(FLOW_ID);
    final int flowVersion = data.getInt(FLOW_VERSION);
    final String submitUser = data.getString(SUBMIT_USER);
    final FlowTrigger flowTrigger = (FlowTrigger) data.get(FLOW_TRIGGER);
    this.triggerService.startTrigger(flowTrigger, flowId, flowVersion, submitUser, project);
  }
}


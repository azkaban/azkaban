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

import static java.util.Objects.requireNonNull;

import azkaban.flow.Flow;
import azkaban.project.FlowLoaderUtils;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.scheduler.QuartzJobDescription;
import azkaban.scheduler.QuartzScheduler;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class FlowTriggerScheduler {

  private static final Logger logger = LoggerFactory.getLogger(FlowTriggerScheduler.class);
  private final ProjectLoader projectLoader;
  private final QuartzScheduler scheduler;
  private final ProjectManager projectManager;

  @Inject
  public FlowTriggerScheduler(final ProjectLoader projectLoader, final QuartzScheduler scheduler,
      final ProjectManager projectManager) {
    this.projectLoader = requireNonNull(projectLoader);
    this.scheduler = requireNonNull(scheduler);
    this.projectManager = requireNonNull(projectManager);
  }

  /**
   * Schedule flows containing flow triggers for this project.
   */
  public void schedule(final Project project, final String submitUser)
      throws ProjectManagerException, IOException, SchedulerException {

    for (final Flow flow : project.getFlows()) {
      //todo chengren311: we should validate embedded flow shouldn't have flow trigger defined.
      if (flow.isEmbeddedFlow()) {
        // skip scheduling embedded flow since embedded flow are not allowed to have flow trigger
        continue;
      }
      final String flowFileName = flow.getId() + ".flow";
      final int latestFlowVersion = this.projectLoader
          .getLatestFlowVersion(flow.getProjectId(), flow
              .getVersion(), flowFileName);
      if (latestFlowVersion > 0) {
        final File tempDir = Files.createTempDir();
        final File flowFile;
        try {
          flowFile = this.projectLoader
              .getUploadedFlowFile(project.getId(), project.getVersion(),
                  flowFileName, latestFlowVersion, tempDir);

          final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);

          if (flowTrigger != null) {
            final Map<String, Object> contextMap = ImmutableMap
                .of(FlowTriggerQuartzJob.SUBMIT_USER, submitUser,
                    FlowTriggerQuartzJob.FLOW_TRIGGER, flowTrigger,
                    FlowTriggerQuartzJob.FLOW_ID, flow.getId(),
                    FlowTriggerQuartzJob.FLOW_VERSION, latestFlowVersion,
                    FlowTriggerQuartzJob.PROJECT_ID, project.getId());
            final boolean scheduleSuccess = this.scheduler
                .scheduleJobIfAbsent(flowTrigger.getSchedule().getCronExpression(),
                    new QuartzJobDescription
                    (FlowTriggerQuartzJob.class, FlowTriggerQuartzJob.JOB_NAME,
                        generateGroupName(flow), contextMap));
            if (scheduleSuccess) {
              logger.info("Successfully registered flow {}.{} to scheduler", project.getName(),
                  flow.getId());
            } else {
              logger.info("Fail to register a duplicate flow {}.{} to scheduler", project.getName(),
                  flow.getId());
            }
          }
        } catch (final SchedulerException | IOException ex) {
          logger.error("Error in registering flow {}.{}", project.getName(), flow.getId(), ex);
          throw ex;
        } finally {
          FlowLoaderUtils.cleanUpDir(tempDir);
        }
      }
    }
  }

  public boolean pauseFlowTriggerIfPresent(final int projectId, final String flowId)
      throws SchedulerException {
    return this.scheduler
        .pauseJobIfPresent(FlowTriggerQuartzJob.JOB_NAME, generateGroupName(projectId, flowId));
  }

  public boolean resumeFlowTriggerIfPresent(final int projectId, final String flowId) throws
      SchedulerException {
    return this.scheduler
        .resumeJobIfPresent(FlowTriggerQuartzJob.JOB_NAME, generateGroupName(projectId, flowId));
  }

  /**
   * Retrieve the list of scheduled flow triggers from quartz database
   */
  public List<ScheduledFlowTrigger> getScheduledFlowTriggerJobs() {
    try {
      final Scheduler quartzScheduler = this.scheduler.getScheduler();
      final List<String> groupNames = quartzScheduler.getJobGroupNames();

      final List<ScheduledFlowTrigger> flowTriggerJobDetails = new ArrayList<>();
      for (final String groupName : groupNames) {
        final JobKey jobKey = new JobKey(FlowTriggerQuartzJob.JOB_NAME, groupName);
        ScheduledFlowTrigger scheduledFlowTrigger = null;
        try {
          final JobDetail job = quartzScheduler.getJobDetail(jobKey);
          final JobDataMap jobDataMap = job.getJobDataMap();

          final String flowId = jobDataMap.getString(FlowTriggerQuartzJob.FLOW_ID);
          final int projectId = jobDataMap.getInt(FlowTriggerQuartzJob.PROJECT_ID);
          final FlowTrigger flowTrigger = (FlowTrigger) jobDataMap
              .get(FlowTriggerQuartzJob.FLOW_TRIGGER);
          final String submitUser = jobDataMap.getString(FlowTriggerQuartzJob.SUBMIT_USER);
          final List<? extends Trigger> quartzTriggers = quartzScheduler.getTriggersOfJob(jobKey);
          final boolean isPaused = this.scheduler
              .isJobPaused(FlowTriggerQuartzJob.JOB_NAME, groupName);
          scheduledFlowTrigger = new ScheduledFlowTrigger(projectId,
              this.projectManager.getProject(projectId).getName(),
              flowId, flowTrigger, submitUser, quartzTriggers.isEmpty() ? null
              : quartzTriggers.get(0), isPaused);
        } catch (final Exception ex) {
          logger.error("Unable to get flow trigger by job key {}", jobKey, ex);
          scheduledFlowTrigger = null;
        }

        flowTriggerJobDetails.add(scheduledFlowTrigger);
      }
      return flowTriggerJobDetails;
    } catch (final Exception ex) {
      logger.error("Unable to get scheduled flow triggers", ex);
      return new ArrayList<>();
    }
  }

  /**
   * Unschedule all possible flows in a project
   */
  public void unschedule(final Project project) throws SchedulerException {
    for (final Flow flow : project.getFlows()) {
      if (!flow.isEmbeddedFlow()) {
        try {
          if (this.scheduler
              .unscheduleJob(FlowTriggerQuartzJob.JOB_NAME, generateGroupName(flow))) {
            logger.info("Flow {}.{} unregistered from scheduler", project.getName(), flow.getId());
          }
        } catch (final SchedulerException e) {
          logger.error("Fail to unregister flow from scheduler {}.{}", project.getName(),
              flow.getId(), e);
          throw e;
        }
      }
    }
  }

  private String generateGroupName(final Flow flow) {
    return generateGroupName(flow.getProjectId(), flow.getId());
  }

  private String generateGroupName(final int projectId, final String flowId) {
    return String.valueOf(projectId) + "." + flowId;
  }

  public void start() throws SchedulerException {
    this.scheduler.start();
  }

  public void shutdown() throws SchedulerException {
    this.scheduler.shutdown();
  }

  public static class ScheduledFlowTrigger {

    private final int projectId;
    private final String projectName;
    private final String flowId;
    private final FlowTrigger flowTrigger;
    private final Trigger quartzTrigger;
    private final String submitUser;
    private final boolean isPaused;

    public ScheduledFlowTrigger(final int projectId, final String projectName, final String flowId,
        final FlowTrigger flowTrigger, final String submitUser,
        final Trigger quartzTrigger, final boolean isPaused) {
      this.projectId = projectId;
      this.projectName = projectName;
      this.flowId = flowId;
      this.flowTrigger = flowTrigger;
      this.submitUser = submitUser;
      this.quartzTrigger = quartzTrigger;
      this.isPaused = isPaused;
    }

    public boolean isPaused() {
      return this.isPaused;
    }

    public int getProjectId() {
      return this.projectId;
    }

    public String getProjectName() {
      return this.projectName;
    }

    public String getFlowId() {
      return this.flowId;
    }

    public FlowTrigger getFlowTrigger() {
      return this.flowTrigger;
    }

    public String getDependencyListJson() {
      return new GsonBuilder().setPrettyPrinting().create()
          .toJson(this.flowTrigger.getDependencies());
    }

    public Trigger getQuartzTrigger() {
      return this.quartzTrigger;
    }

    public String getSubmitUser() {
      return this.submitUser;
    }
  }
}

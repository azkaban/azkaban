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

import azkaban.flow.Flow;
import azkaban.project.*;
import azkaban.scheduler.QuartzJobDescription;
import azkaban.scheduler.QuartzScheduler;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.gson.GsonBuilder;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

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
     * Schedule flows containing flow triggers
     */
    public void scheduleAll(final Project project, final String submitUser)
            throws SchedulerException, ProjectManagerException {

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
                        logger.info("scheduling flow " + flow.getProjectId() + "." + flow.getId());
                        this.scheduler
                                .registerJob(flowTrigger.getSchedule().getCronExpression(), new QuartzJobDescription
                                        (FlowTriggerQuartzJob.class, FlowTriggerQuartzJob.JOB_NAME,
                                                generateGroupName(flow), contextMap));
                    }
                } catch (final Exception ex) {
                    logger.error(String.format("error in registering flow [project: %s, flow: %s]", project
                            .getName(), flow.getId()), ex);
                } finally {
                    FlowLoaderUtils.cleanUpDir(tempDir);
                }
            }
        }
    }

    public void pauseFlowTrigger(final int projectId, final String flowId) throws SchedulerException {
        logger.info(String.format("pausing flow trigger for [projectId:%s, flowId:%s]", projectId,
                flowId));
        this.scheduler.pauseJob(FlowTriggerQuartzJob.JOB_NAME, generateGroupName(projectId, flowId));
    }

    public void resumeFlowTrigger(final int projectId, final String flowId) throws
            SchedulerException {
        logger.info(
                String.format("resuming flow trigger for [projectId:%s, flowId:%s]", projectId, flowId));
        this.scheduler.resumeJob(FlowTriggerQuartzJob.JOB_NAME, generateGroupName(projectId, flowId));
    }

    /**
     * Retrieve the list of scheduled flow triggers from quartz database
     */
    public List<ScheduledFlowTrigger> getScheduledFlowTriggerJobs() {
        try {
            final Scheduler quartzScheduler = this.scheduler.getScheduler();
            logger.info("0");
            final List<String> groupNames = quartzScheduler.getJobGroupNames();
            logger.info(groupNames.toString());
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
                    logger.error(String.format("unable to get flow trigger by job key %s", jobKey), ex);
                    scheduledFlowTrigger = null;
                }

                flowTriggerJobDetails.add(scheduledFlowTrigger);
            }
            return flowTriggerJobDetails;
        } catch (final Exception ex) {
            logger.error("unable to get scheduled flow triggers", ex);
            return new ArrayList<>();
        }
    }

    /**
     * Unschedule all possible flows in a project
     */
    public void unscheduleAll(final Project project) throws SchedulerException {
        for (final Flow flow : project.getFlows()) {
            logger.info("unscheduling flow" + flow.getProjectId() + "." + flow.getId() + " if it has "
                    + " schedule");
            if (!flow.isEmbeddedFlow()) {
                try {
                    this.scheduler.unregisterJob(FlowTriggerQuartzJob.JOB_NAME, generateGroupName(flow));
                } catch (final Exception ex) {
                    logger.info("error when unregistering job", ex);
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

    public void start() {
        this.scheduler.start();
    }

    public void shutdown() {
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

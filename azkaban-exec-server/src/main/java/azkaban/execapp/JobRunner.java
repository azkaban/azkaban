/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.execapp;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_ADD_GROUP_AND_USER_FOR_EFFECTIVE_USER;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_WEBSERVER_URL;
import static azkaban.utils.ExecuteAsUserUtils.addGroupAndUserForEffectiveUser;

import azkaban.Constants;
import azkaban.Constants.JobProperties;
import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.execapp.FlowRunner.FlowRunnerProxy;
import azkaban.execapp.event.BlockingStatus;
import azkaban.execapp.event.FlowWatcher;
import azkaban.executor.ClusterInfo;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.JobRunnerBase;
import azkaban.executor.Status;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobtype.JobTypeManager;
import azkaban.jobtype.JobTypeManagerException;
import azkaban.logs.ExecutionLogsLoader;
import azkaban.spi.EventType;
import azkaban.utils.ExecuteAsUser;
import azkaban.utils.KafkaLog4jUtils;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.log4jappender.KafkaLog4jAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.RollingFileAppender;

public class JobRunner extends JobRunnerBase implements Runnable {
  private static final Logger serverLogger = Logger.getLogger(JobRunner.class);
  private static final Object logCreatorLock = new Object();

  private static final String DEFAULT_LAYOUT =
      "%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n";

  private final Object syncObject = new Object();
  private final JobTypeManager jobtypeManager;
  private final ExecutorLoader executorLoader;
  private final ExecutionLogsLoader executionLogsLoader;
  private final Props azkabanProps;
  private final ExecutableNode node;
  private final File workingDir;
  private final Layout loggerLayout;
  private final String jobId;
  private final Set<String> pipelineJobs = new HashSet<>();
  private final FlowRunnerProxy flowRunnerProxy;
  private Logger flowLogger = null;
  private Appender jobAppender = null;
  private KafkaLog4jAppender kafkaLog4jAppender;
  private File logFile;
  private String attachmentFileName;
  private Job job;
  private int executionId = -1;
  // Used by the job to watch and block against another flow
  private Integer pipelineLevel = null;
  private FlowWatcher watcher = null;
  private Set<String> proxyUsers = null;
  private String effectiveUser = null;

  private String jobLogChunkSize;
  private int jobLogBackupIndex;

  private long delayStartMs = 0;
  private volatile boolean killed = false;
  private BlockingStatus currentBlockStatus = null;
  private final ClassLoader threadClassLoader;

  private volatile long timeInQueue = -1;
  private volatile long jobKillTime = -1;

  private volatile long queueDuration = 0;
  private volatile long killDuration = 0;

  public JobRunner(final ExecutableNode node, final File workingDir,
      final ExecutorLoader executorLoader, final ExecutionLogsLoader executionLogsLoader,
      final JobTypeManager jobtypeManager, final Props azkabanProps, final FlowRunnerProxy flowRunnerProxy) {
    super(node.getInputProps());
    this.node = node;
    this.workingDir = workingDir;

    this.executionId = node.getParentFlow().getExecutionId();
    this.jobId = node.getId();

    this.flowRunnerProxy = flowRunnerProxy;

    this.executorLoader = executorLoader;
    this.executionLogsLoader = executionLogsLoader;
    this.jobtypeManager = jobtypeManager;
    this.azkabanProps = azkabanProps;
    final String jobLogLayout = this.props.getString(
        JobProperties.JOB_LOG_LAYOUT, DEFAULT_LAYOUT);

    this.loggerLayout = new EnhancedPatternLayout(jobLogLayout);
    this.threadClassLoader = Thread.currentThread().getContextClassLoader();
  }

  @VisibleForTesting
  FlowRunnerProxy getFlowRunnerProxy() {
    return this.flowRunnerProxy;
  }

  public static String createLogFileName(final ExecutableNode node, final int attempt) {
    final int executionId = node.getExecutableFlow().getExecutionId();
    String jobId = node.getId();
    if (node.getExecutableFlow() != node.getParentFlow()) {
      // Posix safe file delimiter
      jobId = node.getPrintableId("._.");
    }
    return attempt > 0 ? "_job." + executionId + "." + attempt + "." + jobId
        + ".log" : "_job." + executionId + "." + jobId + ".log";
  }

  public static String createLogFileName(final ExecutableNode node) {
    return JobRunner.createLogFileName(node, node.getAttempt());
  }

  public static String createMetaDataFileName(final ExecutableNode node, final int attempt) {
    final int executionId = node.getExecutableFlow().getExecutionId();
    String jobId = node.getId();
    if (node.getExecutableFlow() != node.getParentFlow()) {
      // Posix safe file delimiter
      jobId = node.getPrintableId("._.");
    }

    return attempt > 0 ? "_job." + executionId + "." + attempt + "." + jobId
        + ".meta" : "_job." + executionId + "." + jobId + ".meta";
  }

  public static String createMetaDataFileName(final ExecutableNode node) {
    return JobRunner.createMetaDataFileName(node, node.getAttempt());
  }

  public static String createAttachmentFileName(final ExecutableNode node) {

    return JobRunner.createAttachmentFileName(node, node.getAttempt());
  }

  public static String createAttachmentFileName(final ExecutableNode node, final int attempt) {
    final int executionId = node.getExecutableFlow().getExecutionId();
    String jobId = node.getId();
    if (node.getExecutableFlow() != node.getParentFlow()) {
      // Posix safe file delimiter
      jobId = node.getPrintableId("._.");
    }

    return attempt > 0 ? "_job." + executionId + "." + attempt + "." + jobId
        + ".attach" : "_job." + executionId + "." + jobId + ".attach";
  }

  public void setValidatedProxyUsers(final Set<String> proxyUsers) {
    this.proxyUsers = proxyUsers;
  }

  public void setLogSettings(final Logger flowLogger, final String logFileChuckSize,
      final int numLogBackup) {
    this.flowLogger = flowLogger;
    this.jobLogChunkSize = logFileChuckSize;
    this.jobLogBackupIndex = numLogBackup;
  }

  public String getEffectiveUser() {
    return this.effectiveUser;
  }

  /**
   * Determine the effective user for the job.
   * <p>
   * If the jobType for this job has a defaultProxyUser then return that. If the user.to.proxy is
   * not specified in the job props, then return submit user. If the user.to.proxy is specified in
   * the Job, and - It is also part of list of proxy users in the project permissions, then return
   * the same. - Otherwise return Optional.empty()
   *
   * @return {@link Optional} effective user for the Job
   */
  public Optional<String> determineEffectiveUser(Optional<String> jobType) {
    if (jobType.isPresent()) {
      // JobTypePluginSet is never NULL
      Optional<String> defaultProxyUser = this.jobtypeManager.getJobTypePluginSet()
          .getDefaultProxyUser(jobType.get());
      if (defaultProxyUser.isPresent()) {
        this.logger.info(String.format("Found default proxy user %s for the job type %s",
            defaultProxyUser.get(), jobType.get()));
        return defaultProxyUser;
      }
    }

    if (this.props.containsKey(JobProperties.USER_TO_PROXY)) {
      final String jobProxyUser = this.props.getString(JobProperties.USER_TO_PROXY);
      if (this.proxyUsers != null && !this.proxyUsers.contains(jobProxyUser)) {
        final String permissionsPageURL = getProjectPermissionsURL();
        this.logger.error("User " + jobProxyUser
            + " has no permission to execute this job " + this.jobId + "!"
            + " If you want to execute this flow as " + jobProxyUser
            + ", please add it to Proxy Users under project permissions page: " +
            permissionsPageURL);
        return Optional.empty();
      }
      return Optional.of(jobProxyUser);
    } else {
      final String submitUser = this.getNode().getExecutableFlow().getSubmitUser();
      this.logger.info("user.to.proxy property was not set, defaulting to submit user " +
          submitUser);
      return Optional.ofNullable(submitUser);
    }
  }

  public void setPipeline(final FlowWatcher watcher, final int pipelineLevel) {
    this.watcher = watcher;
    this.pipelineLevel = pipelineLevel;

    if (this.pipelineLevel == 1) {
      this.pipelineJobs.add(this.node.getNestedId());
    } else if (this.pipelineLevel == 2) {
      this.pipelineJobs.add(this.node.getNestedId());
      final ExecutableFlowBase parentFlow = this.node.getParentFlow();

      if (parentFlow.getEndNodes().contains(this.node.getId())) {
        if (!parentFlow.getOutNodes().isEmpty()) {
          final ExecutableFlowBase grandParentFlow = parentFlow.getParentFlow();
          for (final String outNode : parentFlow.getOutNodes()) {
            final ExecutableNode nextNode =
                grandParentFlow.getExecutableNode(outNode);

            // If the next node is a nested flow, then we add the nested
            // starting nodes
            if (nextNode instanceof ExecutableFlowBase) {
              final ExecutableFlowBase nextFlow = (ExecutableFlowBase) nextNode;
              findAllStartingNodes(nextFlow, this.pipelineJobs);
            } else {
              this.pipelineJobs.add(nextNode.getNestedId());
            }
          }
        }
      } else {
        for (final String outNode : this.node.getOutNodes()) {
          final ExecutableNode nextNode = parentFlow.getExecutableNode(outNode);

          // If the next node is a nested flow, then we add the nested starting
          // nodes
          if (nextNode instanceof ExecutableFlowBase) {
            final ExecutableFlowBase nextFlow = (ExecutableFlowBase) nextNode;
            findAllStartingNodes(nextFlow, this.pipelineJobs);
          } else {
            this.pipelineJobs.add(nextNode.getNestedId());
          }
        }
      }
    } else if (this.pipelineLevel == 3) {
      final ExecutableFlowBase parentFlow = findRootParentFlow(this.node.getParentFlow());
      findAllEndingNodes(parentFlow, this.pipelineJobs);
    }
  }

  private ExecutableFlowBase findRootParentFlow(final ExecutableFlowBase node) {
    if (node.getParentFlow() != null) {
      return findRootParentFlow(node.getParentFlow());
    }
    return node;
  }

  private void findAllStartingNodes(final ExecutableFlowBase flow,
      final Set<String> pipelineJobs) {
    for (final String startingNode : flow.getStartNodes()) {
      final ExecutableNode node = flow.getExecutableNode(startingNode);
      if (node instanceof ExecutableFlowBase) {
        findAllStartingNodes((ExecutableFlowBase) node, pipelineJobs);
      } else {
        pipelineJobs.add(node.getNestedId());
      }
    }
  }

  private void findAllEndingNodes(final ExecutableFlowBase flow,
      final Set<String> pipelineJobs) {
    for (final String endingNode : flow.getEndNodes()) {
      final ExecutableNode node = flow.getExecutableNode(endingNode);
      if (node instanceof ExecutableFlowBase) {
        findAllEndingNodes((ExecutableFlowBase) node, pipelineJobs);
      } else {
        pipelineJobs.add(node.getNestedId());
      }
    }
  }

  /**
   * Returns a list of jobs that this JobRunner will wait upon to finish before starting. It is only
   * relevant if pipeline is turned on.
   */
  public Set<String> getPipelineWatchedJobs() {
    return this.pipelineJobs;
  }

  public long getDelayStart() {
    return this.delayStartMs;
  }

  public void setDelayStart(final long delayMS) {
    this.delayStartMs = delayMS;
  }

  public ExecutableNode getNode() {
    return this.node;
  }

  public String getJobId() {
    return this.node.getId();
  }

  public String getLogFilePath() {
    return this.logFile == null ? null : this.logFile.getPath();
  }

  private void createLogger() {
    // Create logger
    synchronized (logCreatorLock) {
      final String loggerName =
          System.currentTimeMillis() + "." + this.executionId + "."
              + this.jobId;
      this.logger = Logger.getLogger(loggerName);

      try {
        attachFileAppender(createFileAppender());
      } catch (final IOException e) {
        removeAppender(this.jobAppender);
        this.flowLogger.error("Could not open log file in " + this.workingDir
            + " for job " + this.jobId, e);
      }

      if (this.azkabanProps.getBoolean(Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_ENABLED, false)) {
        // Keep the names consistent as what we did in uploadLogFile()
        this.kafkaLog4jAppender =
            KafkaLog4jUtils.getAzkabanJobKafkaLog4jAppender(this.azkabanProps, this.loggerLayout,
                String.valueOf(this.executionId), this.node.getNestedId(),
                String.valueOf(this.node.getAttempt()));

        if (this.kafkaLog4jAppender != null) {
          this.logger.addAppender(this.kafkaLog4jAppender);
          this.logger.setAdditivity(false);
          this.flowLogger.info("Attached new Kafka appender for job " + this.jobId);
        }
      }
    }
  }

  private void attachFileAppender(final FileAppender appender) {
    // If present, remove the existing file appender
    assert (this.jobAppender == null);

    this.jobAppender = appender;
    this.logger.addAppender(this.jobAppender);
    this.logger.setAdditivity(false);
    this.flowLogger.info("Attached file appender for job " + this.jobId);
  }

  private FileAppender createFileAppender() throws IOException {
    // Set up log files
    final String logName = createLogFileName(this.node);
    this.logFile = new File(this.workingDir, logName);
    final String absolutePath = this.logFile.getAbsolutePath();
    this.flowLogger.info("Log file path for job: " + this.jobId + " is: " + absolutePath);

    // Attempt to create FileAppender
    final RollingFileAppender fileAppender =
        new RollingFileAppender(this.loggerLayout, absolutePath, true);
    fileAppender.setMaxBackupIndex(this.jobLogBackupIndex);
    fileAppender.setMaxFileSize(this.jobLogChunkSize);

    this.flowLogger.info("Created file appender for job " + this.jobId);
    return fileAppender;
  }

  private void createAttachmentFile() {
    final String fileName = createAttachmentFileName(this.node);
    final File file = new File(this.workingDir, fileName);
    this.attachmentFileName = file.getAbsolutePath();
  }

  private void removeAppender(final Appender appender) {
    if (appender != null) {
      try {
        this.logger.removeAppender(appender);
        appender.close();
      } catch (Exception e) {
        logger.error("Failed to remove appender " + appender.getName(), e);
      }
    }
  }

  private void closeLogger() {
    removeAppender(this.jobAppender);
    removeAppender(this.kafkaLog4jAppender);
  }

  private void writeStatus() {
    try {
      this.node.setUpdateTime(System.currentTimeMillis());
      this.executorLoader.updateExecutableNode(this.node);
    } catch (final ExecutorManagerException e) {
      this.flowLogger.error("Could not update job properties in db for "
          + this.jobId, e);
    }
  }

  /**
   * Used to handle non-ready and special status's (i.e. KILLED). Returns true if they handled
   * anything.
   */
  private boolean handleNonReadyStatus() {
    synchronized (this.syncObject) {
      Status nodeStatus = this.node.getStatus();
      boolean quickFinish = false;
      final long time = System.currentTimeMillis();

      if (Status.isStatusFinished(nodeStatus)) {
        quickFinish = true;
      } else if (nodeStatus == Status.DISABLED) {
        changeStatus(Status.SKIPPED, time);
        quickFinish = true;
      } else if (this.isKilled()) {
        changeStatus(Status.KILLED, time);
        quickFinish = true;
      }

      if (quickFinish) {
        this.node.setStartTime(time);
        fireEvent(Event.create(this, EventType.JOB_STARTED, new EventData(node)));
        this.node.setEndTime(time);
        fireEvent(Event.create(this, EventType.JOB_FINISHED, new EventData(node)));
        return true;
      }

      return false;
    }
  }

  /**
   * If pipelining is set, will block on another flow's jobs.
   */
  private boolean blockOnPipeLine() {
    if (this.isKilled()) {
      return true;
    }

    // For pipelining of jobs. Will watch other jobs.
    if (!this.pipelineJobs.isEmpty()) {
      String blockedList = "";
      final ArrayList<BlockingStatus> blockingStatus =
          new ArrayList<>();
      for (final String waitingJobId : this.pipelineJobs) {
        final Status status = this.watcher.peekStatus(waitingJobId);
        if (status != null && !Status.isStatusFinished(status)) {
          final BlockingStatus block = this.watcher.getBlockingStatus(waitingJobId);
          blockingStatus.add(block);
          blockedList += waitingJobId + ",";
        }
      }
      if (!blockingStatus.isEmpty()) {
        this.logger.info("Pipeline job " + this.jobId + " waiting on " + blockedList
            + " in execution " + this.watcher.getExecId());

        for (final BlockingStatus bStatus : blockingStatus) {
          this.logger.info("Waiting on pipelined job " + bStatus.getJobId());
          this.currentBlockStatus = bStatus;
          bStatus.blockOnFinishedStatus();
          if (this.isKilled()) {
            this.logger.info("Job was killed while waiting on pipeline. Quitting.");
            return true;
          } else {
            this.logger.info("Pipelined job " + bStatus.getJobId() + " finished.");
          }
        }
      }
    }

    this.currentBlockStatus = null;
    return false;
  }

  private boolean delayExecution() {
    synchronized (this) {
      if (this.isKilled()) {
        return true;
      }

      final long currentTime = System.currentTimeMillis();
      if (this.delayStartMs > 0) {
        this.logger.info("Delaying start of execution for " + this.delayStartMs
            + " milliseconds.");
        try {
          this.wait(this.delayStartMs);
          this.logger.info("Execution has been delayed for " + this.delayStartMs
              + " ms. Continuing with execution.");
        } catch (final InterruptedException e) {
          this.logger.error("Job " + this.jobId + " was to be delayed for "
              + this.delayStartMs + ". Interrupted after "
              + (System.currentTimeMillis() - currentTime));
        }

        if (this.isKilled()) {
          this.logger.info("Job was killed while in delay. Quitting.");
          return true;
        }
      }
    }

    return false;
  }

  private void finalizeLogFile(final int attemptNo) {
    closeLogger();
    this.flowLogger.debug("Logger has been closed");
    if (this.logFile == null) {
      this.flowLogger.info("Log file for job " + this.jobId + " is null");
      return;
    }

    try {
      final File[] files = this.logFile.getParentFile().listFiles(new FilenameFilter() {
        @Override
        public boolean accept(final File dir, final String name) {
          return name.startsWith(JobRunner.this.logFile.getName());
        }
      });
      Arrays.sort(files, Collections.reverseOrder());

      this.executionLogsLoader.uploadLogFile(this.executionId, this.node.getNestedId(), attemptNo,
          files);
    } catch (final ExecutorManagerException e) {
      this.flowLogger.error(
          "Error writing out logs for job " + this.node.getNestedId(), e);
    }
  }

  private void finalizeAttachmentFile() {
    if (this.attachmentFileName == null) {
      this.flowLogger.info("Attachment file for job " + this.jobId + " is null");
      return;
    }

    try {
      final File file = new File(this.attachmentFileName);
      if (!file.exists()) {
        this.flowLogger.info("No attachment file for job " + this.jobId
            + " written.");
        return;
      }
      this.executorLoader.uploadAttachmentFile(this.node, file);
    } catch (final ExecutorManagerException e) {
      this.flowLogger.error(
          "Error writing out attachment for job " + this.node.getNestedId(), e);
    }
  }

  /**
   * The main run thread.
   */
  @Override
  public void run() {
    try {
      doRun();
    } catch (final Exception e) {
      serverLogger.error("Unexpected exception", e);
      throw e;
    } finally {
      Thread.currentThread().setContextClassLoader(this.threadClassLoader);
      this.flowLogger.debug("Main run thread for job " + this.jobId + " is terminated");
    }
  }

  private void doRun() {
    Thread.currentThread().setName(
        "JobRunner-" + this.jobId + "-" + this.executionId);

    // If the job is cancelled, disabled, killed. No log is created in this case
    if (handleNonReadyStatus()) {
      return;
    }

    createAttachmentFile();
    createLogger();
    boolean errorFound = false;
    // Delay execution if necessary. Will return a true if something went wrong.
    errorFound |= delayExecution();

    // For pipelining of jobs. Will watch other jobs. Will return true if
    // something went wrong.
    errorFound |= blockOnPipeLine();

    // Start the node.
    this.node.setStartTime(System.currentTimeMillis());
    uploadExecutableNode();
    if (!errorFound && !isKilled()) {
      // End of job in queue and start of execution
      if (this.getTimeInQueue() != -1 && this.getQueueDuration() == 0) {
        this.setQueueDuration(System.currentTimeMillis() - this.getTimeInQueue());
      }
      fireEvent(Event.create(this, EventType.JOB_STARTED, new EventData(this.node)));

      if (prepareJob()) {
        // Writes status to the db
        writeStatus();
        fireEvent(Event.create(this, EventType.JOB_STATUS_CHANGED, new EventData(node)));
        try {
          addGroupAndUser(this.jobtypeManager.getCommonPluginLoadProps());
        } catch (Exception e) {
          changeStatus(Status.FAILED);
          logError("Job run failed while adding group and user.");
        }
        runJob();
      } else {
        changeStatus(Status.FAILED);
        logError("Job run failed preparing the job.");
      }
    }
    this.node.setEndTime(System.currentTimeMillis());

    if (isKilled()) {
      // even if it's killed, there is a chance that the job failed is marked as
      // failure,
      // So we set it to KILLED to make sure we know that we forced kill it
      // rather than
      // it being a legitimate failure.
      changeStatus(Status.KILLED);
      if (this.getJobKillTime() != -1 && this.getKillDuration() == 0) {
        this.setKillDuration(System.currentTimeMillis() - this.getJobKillTime());
      }
    }

    logInfo(
        "Finishing job " + this.jobId + getNodeRetryLog() + " at " + this.node.getEndTime()
            + " with status " + this.node.getStatus());

    try {
      this.flowLogger.debug("Finalizing log file for job " + this.jobId);
      finalizeLogFile(this.node.getAttempt());
      this.flowLogger.debug("Finalizing attachment file for job " + this.jobId);
      finalizeAttachmentFile();
      this.flowLogger.debug("Writing node status to DB for job " + this.jobId);
      writeStatus();
    } finally {
      this.flowLogger.debug("Firing JOB_FINISHED events for job " + this.jobId);
      try {
        // note that FlowRunner thread does node.attempt++ when it receives the JOB_FINISHED event
        fireEvent(Event.create(this, EventType.JOB_FINISHED, new EventData(node)), false);
      } catch (final RuntimeException e) {
        serverLogger.warn("Error in fireEvent for JOB_FINISHED for execId:" + this.executionId
            + " jobId: " + this.jobId);
        serverLogger.warn(e.getMessage(), e);
      }
    }
  }

  private String getNodeRetryLog() {
    return this.node.getAttempt() > 0 ? (" retry: " + this.node.getAttempt()) : "";
  }

  private void uploadExecutableNode() {
    try {
      this.executorLoader.uploadExecutableNode(this.node, this.props);
    } catch (final ExecutorManagerException e) {
      this.logger.error("Error writing initial node properties", e);
    }
  }

  private boolean prepareJob() throws RuntimeException {
    // Check pre conditions
    if (this.props == null || this.isKilled()) {
      logError("Failing job. The job properties don't exist");
      return false;
    }

    synchronized (this.syncObject) {
      if (this.node.getStatus() == Status.FAILED || this.isKilled()) {
        return false;
      }

      logInfo("Starting job " + this.jobId + getNodeRetryLog() + " at " + this.node.getStartTime());

      // If it's an embedded flow, we'll add the nested flow info to the job
      // conf
      if (this.node.getExecutableFlow() != this.node.getParentFlow()) {
        final String subFlow = this.node.getPrintableId(":");
        this.props.put(CommonJobProperties.NESTED_FLOW_PATH, subFlow);
      }

      insertJobMetadata();
      insertJVMAargs();

      this.props.put(CommonJobProperties.JOB_ID, this.jobId);
      this.props.put(CommonJobProperties.JOB_ATTEMPT, this.node.getAttempt());
      this.props.put(CommonJobProperties.JOB_METADATA_FILE,
          createMetaDataFileName(this.node));
      this.props.put(CommonJobProperties.JOB_ATTACHMENT_FILE, this.attachmentFileName);
      this.props.put(CommonJobProperties.JOB_LOG_FILE, this.logFile.getAbsolutePath());
      changeStatus(Status.RUNNING);

      // Ability to specify working directory
      if (this.props.containsKey(AbstractProcessJob.WORKING_DIR)) {
        if (!IsSpecifiedWorkingDirectoryValid()) {
          logError("Specified " + AbstractProcessJob.WORKING_DIR + " is not valid: " +
              this.props.get(AbstractProcessJob.WORKING_DIR) + ". Must be a subdirectory of " +
              this.workingDir.getAbsolutePath());
          return false;
        }
      } else {
        this.props.put(AbstractProcessJob.WORKING_DIR, this.workingDir.getAbsolutePath());
      }
      Optional<String> jobType = JobTypeManager.getJobType(this.props);
      Optional<String> effectiveUserOptional = determineEffectiveUser(jobType);
      // Fail if the effective user is not present
      if (!effectiveUserOptional.isPresent()) {
        return false;
      }
      this.effectiveUser = effectiveUserOptional.get();
      this.props.put(JobProperties.USER_TO_PROXY, this.effectiveUser);
      if (null != this.flowRunnerProxy) {
        this.flowRunnerProxy.setEffectiveUser(this.jobId, this.effectiveUser, jobType);
      }

      final Props props = this.node.getRampProps();
      if (props != null) {
        this.logger.info(String
            .format("RAMP_JOB_ATTACH_PROPS : (id = %s, props = %s)", this.node.getId(),
                props.toString()));
        this.props.putAll(props);
      }

      try {
        long jobCreationStartMillis = System.currentTimeMillis();
        final JobTypeManager.JobParams jobParams = this.jobtypeManager
            .createJobParams(this.jobId, this.props, this.logger);
        Thread.currentThread().setContextClassLoader(jobParams.contextClassLoader);
        this.job = JobTypeManager.createJob(this.jobId, jobParams, this.logger);
        this.logger.info(String.format("%s creation took %s milliseconds.",
            this.jobId, System.currentTimeMillis() - jobCreationStartMillis));

        if (jobParams.jobProps.containsKey(CommonJobProperties.TARGET_CLUSTER_ID)) {
          // save the information of the cluster that the job may be routed to
          final String clusterId = jobParams.jobProps.getString(
              CommonJobProperties.TARGET_CLUSTER_ID);
          final String hadoopClusterURL = jobParams.jobProps.get(
              Constants.ConfigurationKeys.HADOOP_CLUSTER_URL);
          final String rmURL = jobParams.jobProps.get(
              Constants.ConfigurationKeys.RESOURCE_MANAGER_JOB_URL);
          final String hsURL = jobParams.jobProps.get(
              Constants.ConfigurationKeys.HISTORY_SERVER_JOB_URL);
          final String shsURL = jobParams.jobProps.get(
              Constants.ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL);
          final ClusterInfo clusterInfo =
              new ClusterInfo(clusterId, hadoopClusterURL, rmURL, hsURL, shsURL);
          this.node.setClusterInfo(clusterInfo);
        }

      } catch (final JobTypeManagerException e) {
        this.logger.error("Failed to build job type", e);
        return false;
      }
    }

    return true;
  }

  // If linux group and user needs to be added for effectiveUser before job process starts
  // then set it up before effective user is used to set permission for any file.
  private void addGroupAndUser(final Props pluginPrivateProps) throws Exception {
    if (pluginPrivateProps != null) {
      final boolean addGroupAndUserForEffectiveUser =
          pluginPrivateProps.getBoolean(AZKABAN_ADD_GROUP_AND_USER_FOR_EFFECTIVE_USER, false);
      if (addGroupAndUserForEffectiveUser) {
        logger.info("Adding group and user for effective user: " + this.effectiveUser);
        final ExecuteAsUser executeAsUser = new ExecuteAsUser(
            pluginPrivateProps.getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));
        addGroupAndUserForEffectiveUser(executeAsUser, this.effectiveUser);
      }
    }
  }

  /**
   * Validates execution directory specified by user.
   */
  private boolean IsSpecifiedWorkingDirectoryValid() {
    final File usersWorkingDir = new File(this.props.get(AbstractProcessJob.WORKING_DIR));
    try {
      if (!usersWorkingDir.getCanonicalPath().startsWith(this.workingDir.getCanonicalPath())) {
        return false;
      }
    } catch (final IOException e) {
      this.logger.error("Failed to validate user's " + AbstractProcessJob.WORKING_DIR +
          " property.", e);
      return false;
    }
    return true;
  }

  /**
   * Get project permissions page URL
   */
  private String getProjectPermissionsURL() {
    String projectPermissionsURL = null;
    final String baseURL = this.azkabanProps.get(AZKABAN_WEBSERVER_URL);
    if (baseURL != null) {
      final String projectName = this.node.getParentFlow().getProjectName();
      projectPermissionsURL = String
          .format("%s/manager?project=%s&permissions", baseURL, projectName);
    }
    return projectPermissionsURL;
  }


  /**
   * Add useful JVM arguments so it is easier to map a running Java process to a flow, execution id
   * and job
   */
  private void insertJVMAargs() {
    final String flowName = this.node.getParentFlow().getFlowId();
    final String jobId = this.node.getId();

    String jobJVMArgs =
        String.format(
            "'-Dazkaban.flowid=%s' '-Dazkaban.execid=%s' '-Dazkaban.jobid=%s'",
            flowName, this.executionId, jobId);

    final String previousJVMArgs = this.props.get(JavaProcessJob.JVM_PARAMS);
    jobJVMArgs += (previousJVMArgs == null) ? "" : " " + previousJVMArgs;

    // Add useful Java options for java jobs which are provided through properties
    final String javaOpts = insertJavaOptions();
    jobJVMArgs += (javaOpts == null) ? "" : " " + javaOpts;
    this.logger.info("job JVM args: " + jobJVMArgs);
    this.props.put(JavaProcessJob.JVM_PARAMS, jobJVMArgs);
  }

  /**
   * Add useful Java options for java jobs provided through properties.
   */
  private String insertJavaOptions() {
    if (this.jobtypeManager.getCommonPluginLoadProps() == null) {
      return null;
    }
    final String appendJavaOpts = this.jobtypeManager.getCommonPluginLoadProps().getString(
        Constants.AZ_JOBS_JAVA_OPTS, null);
    logger.info("JAVA OPTS appended to each command : " + appendJavaOpts);
    return appendJavaOpts;
  }


  /**
   * Add relevant links to the job properties so that downstream consumers may know what executions
   * initiated their execution.
   */
  private void insertJobMetadata() {
    final String baseURL = this.azkabanProps.get(AZKABAN_WEBSERVER_URL);
    if (baseURL != null) {
      URL webserverURL = null;
      try {
        webserverURL = new URL(baseURL);
      } catch (MalformedURLException e) {
        logger.error(String.format("Exception in parsing url: %s", baseURL), e);
      }
      final String azkabanWebserverHost = (webserverURL != null) ? webserverURL.getHost() : null;
      final String flowName = this.node.getParentFlow().getFlowId();
      final String projectName = this.node.getParentFlow().getProjectName();

      this.props.put(CommonJobProperties.AZKABAN_URL, baseURL);
      this.props.put(CommonJobProperties.AZKABAN_WEBSERVERHOST, azkabanWebserverHost);
      this.props.put(CommonJobProperties.EXECUTION_LINK,
          String.format("%s/executor?execid=%d", baseURL, this.executionId));
      this.props.put(CommonJobProperties.JOBEXEC_LINK, String.format(
          "%s/executor?execid=%d&job=%s", baseURL, this.executionId, this.node.getNestedId()));
      this.props.put(CommonJobProperties.ATTEMPT_LINK, String.format(
          "%s/executor?execid=%d&job=%s&attempt=%d", baseURL, this.executionId,
          this.node.getNestedId(), this.node.getAttempt()));
      this.props.put(CommonJobProperties.WORKFLOW_LINK, String.format(
          "%s/manager?project=%s&flow=%s", baseURL, projectName, flowName));
      this.props.put(CommonJobProperties.JOB_LINK, String.format(
          "%s/manager?project=%s&flow=%s&job=%s", baseURL, projectName,
          flowName, this.jobId));
    } else {
      if (this.logger != null) {
        this.logger.info(AZKABAN_WEBSERVER_URL + " property was not set");
      }
    }
    // out nodes
    this.props.put(CommonJobProperties.OUT_NODES,
        StringUtils.join2(this.node.getOutNodes(), ","));

    // in nodes
    this.props.put(CommonJobProperties.IN_NODES,
        StringUtils.join2(this.node.getInNodes(), ","));
  }

  private Status runJob() {
    Status finalStatus;
    try {
      this.job.run();
      finalStatus = this.node.getStatus();
    } catch (final Throwable e) {
      synchronized (this.syncObject) {
        if (this.props.getBoolean("job.succeed.on.failure", false)) {
          finalStatus = changeStatus(Status.FAILED_SUCCEEDED);
          logError("Job run failed, but will treat it like success.");
          logError(e.getMessage() + " cause: " + e.getCause(), e);
        } else {
          if (isKilled() || this.node.getStatus() == Status.KILLED) {
            finalStatus = Status.KILLED;
            logError("Job run killed!", e);
          } else {
            finalStatus = changeStatus(Status.FAILED);
            logError("Job run failed!", e);
          }
          logError(e.getMessage() + " cause: " + e.getCause());
        }
        this.getNode().setFailureMessage(e.toString());
      }
    }

    if (this.job != null) {
      this.node.setOutputProps(this.job.getJobGeneratedProperties());
    }

    synchronized (this.syncObject) {
      // If the job is still running (but not killed), set the status to Success.
      if (!Status.isStatusFinished(finalStatus) && !isKilled()) {
        finalStatus = changeStatus(Status.SUCCEEDED);
      }
    }
    return finalStatus;
  }

  private Status changeStatus(final Status status) {
    changeStatus(status, System.currentTimeMillis());
    return status;
  }

  private Status changeStatus(final Status status, final long time) {
    this.node.setStatus(status);
    this.node.setUpdateTime(time);
    return status;
  }

  private void fireEvent(final Event event) {
    fireEvent(event, true);
  }

  private void fireEvent(final Event event, final boolean updateTime) {
    if (updateTime) {
      this.node.setUpdateTime(System.currentTimeMillis());
    }
    this.fireEventListeners(event);
  }

  public void killBySLA() {
    synchronized (this.syncObject) {
      kill();
      this.getNode().setModifiedBy("SLA");
      this.getNode().setKilledBySLA(true);
    }
  }

  public void kill() {
    synchronized (this.syncObject) {
      if (Status.isStatusFinished(this.node.getStatus())) {
        return;
      }
      logError("Kill has been called.");
      this.changeStatus(Status.KILLING);
      this.killed = true;

      final BlockingStatus status = this.currentBlockStatus;
      if (status != null) {
        status.unblock();
      }

      // Cancel code here
      if (this.job == null) {
        logError("Job hasn't started yet.");
        // Just in case we're waiting on the delay
        synchronized (this) {
          this.notify();
        }
        return;
      }

      try {
        this.setJobKillTime(System.currentTimeMillis());
        this.job.cancel();
      } catch (final Exception e) {
        logError(e.getMessage());
        logError(
            "Failed trying to cancel job. Maybe it hasn't started running yet or just finished.");
      }

    }
  }

  public boolean isKilled() {
    return this.killed;
  }

  public Status getStatus() {
    return this.node.getStatus();
  }

  private void logError(final String message) {
    if (this.logger != null) {
      this.logger.error(message);
    }
  }

  private void logError(final String message, final Throwable t) {
    if (this.logger != null) {
      this.logger.error(message, t);
    }
  }

  private void logInfo(final String message) {
    if (this.logger != null) {
      this.logger.info(message);
    }
  }

  public File getLogFile() {
    return this.logFile;
  }

  public long getTimeInQueue() { return this.timeInQueue; }

  public void setTimeInQueue(long timeInQueue) { this.timeInQueue = timeInQueue; }

  public long getJobKillTime() { return this.jobKillTime; }

  public void setJobKillTime(long jobKillTime) { this.jobKillTime = jobKillTime; }

  public long getQueueDuration() { return this.queueDuration; }

  public void setQueueDuration(long queueDuration) { this.queueDuration = queueDuration; }

  public long getKillDuration() { return this.killDuration; }

  public void setKillDuration(long killDuration) { this.killDuration = killDuration; }
}

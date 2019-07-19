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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.LocalFlowWatcher;
import azkaban.execapp.event.RemoteFlowWatcher;
import azkaban.execapp.metric.NumFailedFlowMetric;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.jobtype.JobTypeManager;
import azkaban.jobtype.JobTypeManagerException;
import azkaban.metric.MetricReportManager;
import azkaban.metrics.CommonMetrics;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectWhitelist;
import azkaban.project.ProjectWhitelist.WhitelistType;
import azkaban.sla.SlaOption;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.EventType;
import azkaban.storage.StorageManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import azkaban.utils.OsCpuUtil;
import azkaban.utils.Props;
import azkaban.utils.SystemMemoryInfo;
import azkaban.utils.ThreadPoolExecutingListener;
import azkaban.utils.TrackingThreadPool;
import azkaban.utils.UndefinedPropertyException;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.lang.Thread.State;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Execution manager for the server side execution.
 *
 * When a flow is submitted to FlowRunnerManager, it is the {@link Status#PREPARING} status. When a
 * flow is about to be executed by FlowRunner, its status is updated to {@link Status#RUNNING}
 *
 * Two main data structures are used in this class to maintain flows.
 *
 * runningFlows: this is used as a bookkeeping for submitted flows in FlowRunnerManager. It has
 * nothing to do with the executor service that is used to execute the flows. This bookkeeping is
 * used at the time of canceling or killing a flow. The flows in this data structure is removed in
 * the handleEvent method.
 *
 * submittedFlows: this is used to keep track the execution of the flows, so it has the mapping
 * between a Future<?> and an execution id. This would allow us to find out the execution ids of the
 * flows that are in the Status.PREPARING status. The entries in this map is removed once the flow
 * execution is completed.
 */
@Singleton
public class FlowRunnerManager implements EventListener,
    ThreadPoolExecutingListener {

  private static final Logger logger = Logger.getLogger(FlowRunnerManager.class);

  private static final String EXECUTOR_USE_BOUNDED_THREADPOOL_QUEUE = "executor.use.bounded.threadpool.queue";
  private static final String EXECUTOR_THREADPOOL_WORKQUEUE_SIZE = "executor.threadpool.workqueue.size";
  private static final String EXECUTOR_FLOW_THREADS = "executor.flow.threads";
  private static final String FLOW_NUM_JOB_THREADS = "flow.num.job.threads";

  // recently finished secs to clean up. 1 minute
  private static final int RECENTLY_FINISHED_TIME_TO_LIVE = 60 * 1000;

  private static final int DEFAULT_NUM_EXECUTING_FLOWS = 30;
  private static final int DEFAULT_FLOW_NUM_JOB_TREADS = 10;

  // this map is used to store the flows that have been submitted to
  // the executor service. Once a flow has been submitted, it is either
  // in the queue waiting to be executed or in executing state.
  private final Map<Future<?>, Integer> submittedFlows = new ConcurrentHashMap<>();
  private final Map<Integer, FlowRunner> runningFlows = new ConcurrentHashMap<>();
  // keep track of the number of flow being setup({@link createFlowRunner()})
  private final AtomicInteger preparingFlowCount = new AtomicInteger(0);
  private final Map<Integer, ExecutableFlow> recentlyFinishedFlows = new ConcurrentHashMap<>();
  private final TrackingThreadPool executorService;
  private final CleanerThread cleanerThread;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final JobTypeManager jobtypeManager;
  private final FlowPreparer flowPreparer;
  private final TriggerManager triggerManager;
  private final AlerterHolder alerterHolder;
  private final AzkabanEventReporter azkabanEventReporter;
  private final Props azkabanProps;
  private final File executionDirectory;
  private final File projectDirectory;
  private final Object executionDirDeletionSync = new Object();
  private final CommonMetrics commonMetrics;
  private final ExecMetrics execMetrics;

  private final int numThreads;
  private final int numJobThreadPerFlow;
  // We want to limit the log sizes to about 20 megs
  private final String jobLogChunkSize;
  private final int jobLogNumFiles;
  // If true, jobs will validate proxy user against a list of valid proxy users.
  private final boolean validateProxyUser;
  private PollingService pollingService;
  private int threadPoolQueueSize = -1;
  private Props globalProps;
  private long lastCleanerThreadCheckTime = -1;
  // date time of the the last flow submitted.
  private long lastFlowSubmittedDate = 0;
  // Indicate if the executor is set to active.
  private volatile boolean active;

  @Inject
  public FlowRunnerManager(final Props props,
      final ExecutorLoader executorLoader,
      final ProjectLoader projectLoader,
      final StorageManager storageManager,
      final TriggerManager triggerManager,
      final AlerterHolder alerterHolder,
      final CommonMetrics commonMetrics,
      final ExecMetrics execMetrics,
      @Nullable final AzkabanEventReporter azkabanEventReporter) throws IOException {
    this.azkabanProps = props;

    this.azkabanEventReporter = azkabanEventReporter;

    this.executionDirectory = new File(props.getString("azkaban.execution.dir", "executions"));
    if (!this.executionDirectory.exists()) {
      this.executionDirectory.mkdirs();
      setgidPermissionOnExecutionDirectory();
    }
    this.projectDirectory = new File(props.getString("azkaban.project.dir", "projects"));
    if (!this.projectDirectory.exists()) {
      this.projectDirectory.mkdirs();
    }

    // azkaban.temp.dir
    this.numThreads = props.getInt(EXECUTOR_FLOW_THREADS, DEFAULT_NUM_EXECUTING_FLOWS);
    this.numJobThreadPerFlow = props.getInt(FLOW_NUM_JOB_THREADS, DEFAULT_FLOW_NUM_JOB_TREADS);
    this.executorService = createExecutorService(this.numThreads);

    this.executorLoader = executorLoader;
    this.projectLoader = projectLoader;
    this.triggerManager = triggerManager;
    this.alerterHolder = alerterHolder;
    this.commonMetrics = commonMetrics;
    this.execMetrics = execMetrics;

    this.jobLogChunkSize = this.azkabanProps.getString("job.log.chunk.size", "5MB");
    this.jobLogNumFiles = this.azkabanProps.getInt("job.log.backup.index", 4);

    this.validateProxyUser = this.azkabanProps.getBoolean("proxy.user.lock.down", false);

    final String globalPropsPath = props.getString("executor.global.properties", null);
    if (globalPropsPath != null) {
      this.globalProps = new Props(null, globalPropsPath);
    }

    this.jobtypeManager =
        new JobTypeManager(props.getString(
            AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
            JobTypeManager.DEFAULT_JOBTYPEPLUGINDIR), this.globalProps,
            getClass().getClassLoader());

    ProjectCacheCleaner cleaner = null;
    try {
      final double projectCacheSizePercentage =
          props.getDouble(ConfigurationKeys.PROJECT_CACHE_SIZE_PERCENTAGE);
      cleaner = new ProjectCacheCleaner(this.projectDirectory, projectCacheSizePercentage);
    } catch (final UndefinedPropertyException ex) {
    }

    // Create a flow preparer
    this.flowPreparer = new FlowPreparer(storageManager, this.executionDirectory,
        this.projectDirectory, cleaner, this.execMetrics.getProjectCacheHitRatio());

    this.execMetrics.addFlowRunnerManagerMetrics(this);

    this.cleanerThread = new CleanerThread();
    this.cleanerThread.start();

    if (this.azkabanProps.getBoolean(ConfigurationKeys.AZKABAN_POLL_MODEL, false)) {
      this.logger.info("Starting polling service.");
      this.pollingService = new PollingService(this.azkabanProps
          .getLong(ConfigurationKeys.AZKABAN_POLLING_INTERVAL_MS,
              Constants.DEFAULT_AZKABAN_POLLING_INTERVAL_MS),
          new PollingCriteria(this.azkabanProps));
      this.pollingService.start();
    }
  }

  /**
   * Setting the gid bit on the execution directory forces all files/directories created within the
   * directory to be a part of the group associated with the azkaban process. Then, when users
   * create their own files, the azkaban cleanup thread can properly remove them.
   *
   * Java does not provide a standard library api for setting the gid bit because the gid bit is
   * system dependent, so the only way to set this bit is to start a new process and run the shell
   * command "chmod g+s " + execution directory name.
   *
   * Note that this should work on most Linux distributions and MacOS, but will not work on
   * Windows.
   */
  private void setgidPermissionOnExecutionDirectory() throws IOException {
    logger.info("Creating subprocess to run shell command: chmod g+s "
        + this.executionDirectory.toString());
    Runtime.getRuntime().exec("chmod g+s " + this.executionDirectory.toString());
  }

  private TrackingThreadPool createExecutorService(final int nThreads) {
    final boolean useNewThreadPool =
        this.azkabanProps.getBoolean(EXECUTOR_USE_BOUNDED_THREADPOOL_QUEUE, false);
    logger.info("useNewThreadPool: " + useNewThreadPool);

    if (useNewThreadPool) {
      this.threadPoolQueueSize =
          this.azkabanProps.getInt(EXECUTOR_THREADPOOL_WORKQUEUE_SIZE, nThreads);
      logger.info("workQueueSize: " + this.threadPoolQueueSize);

      // using a bounded queue for the work queue. The default rejection policy
      // {@ThreadPoolExecutor.AbortPolicy} is used
      final TrackingThreadPool executor =
          new TrackingThreadPool(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<>(this.threadPoolQueueSize), this);

      return executor;
    } else {
      // the old way of using unbounded task queue.
      // if the running tasks are taking a long time or stuck, this queue
      // will be very very long.
      return new TrackingThreadPool(nThreads, nThreads, 0L,
          TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), this);
    }
  }

  public void setExecutorActive(final boolean isActive, final String host, final int port)
      throws ExecutorManagerException, InterruptedException {
    final Executor executor = this.executorLoader.fetchExecutor(host, port);
    Preconditions.checkState(executor != null, "Unable to obtain self entry in DB");
    if (executor.isActive() != isActive) {
      executor.setActive(isActive);
      this.executorLoader.updateExecutor(executor);
    } else {
      logger.info(
          "Set active action ignored. Executor is already " + (isActive ? "active" : "inactive"));
    }
    this.active = isActive;
    if (!this.active) {
      // When deactivating this executor, this call will wait to return until every thread in {@link
      // #createFlowRunner} has finished. When deploying new executor, old running executor will be
      // deactivated before new one is activated and only one executor is allowed to
      // delete/hard-linking project dirs to avoid race condition described in {@link
      // FlowPreparer#setup}. So to make deactivation process block until flow preparation work
      // finishes guarantees the old executor won't access {@link FlowPreparer#setup} after
      // deactivation.
      waitUntilFlowPreparationFinish();
    }
  }

  /**
   * Wait until ongoing flow preparation work finishes.
   */
  private void waitUntilFlowPreparationFinish() throws InterruptedException {
    final Duration SLEEP_INTERVAL = Duration.ofSeconds(5);
    while (this.preparingFlowCount.intValue() != 0) {
      logger.info(this.preparingFlowCount + " flow(s) is/are still being setup before complete "
          + "deactivation.");
      Thread.sleep(SLEEP_INTERVAL.toMillis());
    }
  }

  public long getLastFlowSubmittedTime() {
    // Note: this is not thread safe and may result in providing dirty data.
    //       we will provide this data as is for now and will revisit if there
    //       is a string justification for change.
    return this.lastFlowSubmittedDate;
  }

  public Props getGlobalProps() {
    return this.globalProps;
  }

  public void setGlobalProps(final Props globalProps) {
    this.globalProps = globalProps;
  }

  public void submitFlow(final int execId) throws ExecutorManagerException {
    if (isAlreadyRunning(execId)) {
      return;
    }

    final FlowRunner runner = createFlowRunner(execId);

    // Check again.
    if (isAlreadyRunning(execId)) {
      return;
    }
    submitFlowRunner(runner);
  }

  private boolean isAlreadyRunning(final int execId) throws ExecutorManagerException {
    if (this.runningFlows.containsKey(execId)) {
      logger.info("Execution " + execId + " is already in running.");
      if (!this.submittedFlows.containsValue(execId)) {
        // Execution had been added to running flows but not submitted - something's wrong.
        // Return a response with error: this is a cue for the dispatcher to retry or finalize the
        // execution as failed.
        throw new ExecutorManagerException("Execution " + execId +
            " is in runningFlows but not in submittedFlows. Most likely submission had failed.");
      }
      // Already running, everything seems fine. Report as a successful submission.
      return true;
    }
    return false;
  }

  /**
   * return whether this execution has useExecutor defined. useExecutor is for running test
   * executions on inactive executor.
   */
  private boolean isExecutorSpecified(final ExecutableFlow flow) {
    return flow.getExecutionOptions().getFlowParameters()
        .containsKey(ExecutionOptions.USE_EXECUTOR);
  }

  private FlowRunner createFlowRunner(final int execId) throws ExecutorManagerException {
    final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(execId);
    if (flow == null) {
      throw new ExecutorManagerException("Error loading flow with exec " + execId);
    }

    // Sets up the project files and execution directory.
    this.preparingFlowCount.incrementAndGet();
    // Record the time between submission, and when the flow preparation/execution starts.
    // Note that since submit time is recorded on the web server, while flow preparation is on
    // the executor, there could be some inaccuracies due to clock skew.
    this.commonMetrics.addQueueWait(System.currentTimeMillis() -
        flow.getExecutableFlow().getSubmitTime());

    final Timer.Context flowPrepTimerContext = this.execMetrics.getFlowSetupTimerContext();

    try {
      if (this.active || isExecutorSpecified(flow)) {
        this.flowPreparer.setup(flow);
      } else {
        // Unset the executor.
        this.executorLoader.unsetExecutorIdForExecution(execId);
        throw new ExecutorManagerException("executor became inactive before setting up the "
            + "flow " + execId);
      }
    } finally {
      this.preparingFlowCount.decrementAndGet();
      flowPrepTimerContext.stop();
    }

    // Setup flow runner
    FlowWatcher watcher = null;
    final ExecutionOptions options = flow.getExecutionOptions();
    if (options.getPipelineExecutionId() != null) {
      final Integer pipelineExecId = options.getPipelineExecutionId();
      final FlowRunner runner = this.runningFlows.get(pipelineExecId);

      if (runner != null) {
        watcher = new LocalFlowWatcher(runner);
      } else {
        // also ends up here if execute is called with pipelineExecId that's not running any more
        // (it could have just finished, for example)
        watcher = new RemoteFlowWatcher(pipelineExecId, this.executorLoader);
      }
    }

    int numJobThreads = this.numJobThreadPerFlow;
    if (options.getFlowParameters().containsKey(FLOW_NUM_JOB_THREADS)) {
      try {
        final int numJobs =
            Integer.valueOf(options.getFlowParameters().get(
                FLOW_NUM_JOB_THREADS));
        if (numJobs > 0 && (numJobs <= numJobThreads || ProjectWhitelist
            .isProjectWhitelisted(flow.getProjectId(),
                WhitelistType.NumJobPerFlow))) {
          numJobThreads = numJobs;
        }
      } catch (final Exception e) {
        throw new ExecutorManagerException(
            "Failed to set the number of job threads "
                + options.getFlowParameters().get(FLOW_NUM_JOB_THREADS)
                + " for flow " + execId, e);
      }
    }

    final FlowRunner runner =
        new FlowRunner(flow, this.executorLoader, this.projectLoader, this.jobtypeManager,
            this.azkabanProps, this.azkabanEventReporter, this.alerterHolder);
    runner.setFlowWatcher(watcher)
        .setJobLogSettings(this.jobLogChunkSize, this.jobLogNumFiles)
        .setValidateProxyUser(this.validateProxyUser)
        .setNumJobThreads(numJobThreads).addListener(this);

    configureFlowLevelMetrics(runner);
    return runner;
  }

  private void submitFlowRunner(final FlowRunner runner) throws ExecutorManagerException {
    this.runningFlows.put(runner.getExecutionId(), runner);
    try {
      // The executorService already has a queue.
      // The submit method below actually returns an instance of FutureTask,
      // which implements interface RunnableFuture, which extends both
      // Runnable and Future interfaces
      final Future<?> future = this.executorService.submit(runner);
      // keep track of this future
      this.submittedFlows.put(future, runner.getExecutionId());
      // update the last submitted time.
      this.lastFlowSubmittedDate = System.currentTimeMillis();
    } catch (final RejectedExecutionException re) {
      this.runningFlows.remove(runner.getExecutionId());
      final StringBuffer errorMsg = new StringBuffer(
          "Azkaban executor can't execute any more flows. ");
      if (this.executorService.isShutdown()) {
        errorMsg.append("The executor is being shut down.");
      }
      throw new ExecutorManagerException(errorMsg.toString(), re);
    }
  }

  /**
   * Configure Azkaban metrics tracking for a new flowRunner instance
   */
  private void configureFlowLevelMetrics(final FlowRunner flowRunner) {
    logger.info("Configuring Azkaban metrics tracking for flow runner object");

    if (MetricReportManager.isAvailable()) {
      final MetricReportManager metricManager = MetricReportManager.getInstance();
      // Adding NumFailedFlow Metric listener
      flowRunner.addListener((NumFailedFlowMetric) metricManager
          .getMetricFromName(NumFailedFlowMetric.NUM_FAILED_FLOW_METRIC_NAME));
    }

  }

  public void cancelJobBySLA(final int execId, final String jobId)
      throws ExecutorManagerException {
    final FlowRunner flowRunner = this.runningFlows.get(execId);

    if (flowRunner == null) {
      throw new ExecutorManagerException("Execution " + execId
          + " is not running.");
    }

    for (final JobRunner jobRunner : flowRunner.getActiveJobRunners()) {
      if (jobRunner.getJobId().equals(jobId)) {
        logger.info("Killing job " + jobId + " in execution " + execId + " by SLA");
        jobRunner.killBySLA();
        break;
      }
    }
  }

  public void cancelFlow(final int execId, final String user)
      throws ExecutorManagerException {
    final FlowRunner runner = this.runningFlows.get(execId);

    if (runner == null) {
      throw new ExecutorManagerException("Execution " + execId
          + " is not running.");
    }

    runner.kill(user);
  }

  public void pauseFlow(final int execId, final String user)
      throws ExecutorManagerException {
    final FlowRunner runner = this.runningFlows.get(execId);

    if (runner == null) {
      throw new ExecutorManagerException("Execution " + execId
          + " is not running.");
    }

    runner.pause(user);
  }

  public void resumeFlow(final int execId, final String user)
      throws ExecutorManagerException {
    final FlowRunner runner = this.runningFlows.get(execId);

    if (runner == null) {
      throw new ExecutorManagerException("Execution " + execId
          + " is not running.");
    }

    runner.resume(user);
  }

  public void retryFailures(final int execId, final String user)
      throws ExecutorManagerException {
    final FlowRunner runner = this.runningFlows.get(execId);

    if (runner == null) {
      throw new ExecutorManagerException("Execution " + execId
          + " is not running.");
    }

    runner.retryFailures(user);
  }

  public ExecutableFlow getExecutableFlow(final int execId) {
    final FlowRunner runner = this.runningFlows.get(execId);
    if (runner == null) {
      return this.recentlyFinishedFlows.get(execId);
    }
    return runner.getExecutableFlow();
  }

  /**
   * delete execution dir pertaining to the given execution id
   */
  private void deleteExecutionDir(final int executionId) {
    synchronized (this.executionDirDeletionSync) {
      final Path flowExecutionDir = Paths.get(this.executionDirectory.toPath().toString(),
          String.valueOf(executionId));
      try {
        FileUtils.deleteDirectory(flowExecutionDir.toFile());
      } catch (final IOException e) {
        logger.warn("Error when deleting directory " + flowExecutionDir.toAbsolutePath() + ".", e);
      }
    }
  }

  @Override
  public void handleEvent(final Event event) {
    if (event.getType() == EventType.FLOW_FINISHED || event.getType() == EventType.FLOW_STARTED) {
      final FlowRunner flowRunner = (FlowRunner) event.getRunner();
      final ExecutableFlow flow = flowRunner.getExecutableFlow();

      if (event.getType() == EventType.FLOW_FINISHED) {
        this.recentlyFinishedFlows.put(flow.getExecutionId(), flow);
        logger.info("Flow " + flow.getExecutionId()
            + " is finished. Adding it to recently finished flows list.");
        this.runningFlows.remove(flow.getExecutionId());
        this.deleteExecutionDir(flow.getExecutionId());
      } else if (event.getType() == EventType.FLOW_STARTED) {
        // add flow level SLA checker
        this.triggerManager
            .addTrigger(flow.getExecutionId(), SlaOption.getFlowLevelSLAOptions(flow
                .getExecutionOptions().getSlaOptions()));
      }
    }
  }

  public LogData readFlowLogs(final int execId, final int startByte, final int length)
      throws ExecutorManagerException {
    final FlowRunner runner = this.runningFlows.get(execId);
    if (runner == null) {
      throw new ExecutorManagerException("Running flow " + execId
          + " not found.");
    }

    final File dir = runner.getExecutionDir();
    if (dir != null && dir.exists()) {
      try {
        synchronized (this.executionDirDeletionSync) {
          if (!dir.exists()) {
            throw new ExecutorManagerException(
                "Execution dir file doesn't exist. Probably has beend deleted");
          }

          final File logFile = runner.getFlowLogFile();
          if (logFile != null && logFile.exists()) {
            return FileIOUtils.readUtf8File(logFile, startByte, length);
          } else {
            throw new ExecutorManagerException("Flow log file doesn't exist.");
          }
        }
      } catch (final IOException e) {
        throw new ExecutorManagerException(e);
      }
    }

    throw new ExecutorManagerException(
        "Error reading file. Log directory doesn't exist.");
  }

  public LogData readJobLogs(final int execId, final String jobId, final int attempt,
      final int startByte, final int length) throws ExecutorManagerException {
    final FlowRunner runner = this.runningFlows.get(execId);
    if (runner == null) {
      throw new ExecutorManagerException("Running flow " + execId
          + " not found.");
    }

    final File dir = runner.getExecutionDir();
    if (dir != null && dir.exists()) {
      try {
        synchronized (this.executionDirDeletionSync) {
          if (!dir.exists()) {
            throw new ExecutorManagerException(
                "Execution dir file doesn't exist. Probably has beend deleted");
          }
          final File logFile = runner.getJobLogFile(jobId, attempt);
          if (logFile != null && logFile.exists()) {
            return FileIOUtils.readUtf8File(logFile, startByte, length);
          } else {
            throw new ExecutorManagerException("Job log file doesn't exist.");
          }
        }
      } catch (final IOException e) {
        throw new ExecutorManagerException(e);
      }
    }

    throw new ExecutorManagerException(
        "Error reading file. Log directory doesn't exist.");
  }

  public List<Object> readJobAttachments(final int execId, final String jobId, final int attempt)
      throws ExecutorManagerException {
    final FlowRunner runner = this.runningFlows.get(execId);
    if (runner == null) {
      throw new ExecutorManagerException("Running flow " + execId
          + " not found.");
    }

    final File dir = runner.getExecutionDir();
    if (dir == null || !dir.exists()) {
      throw new ExecutorManagerException(
          "Error reading file. Log directory doesn't exist.");
    }

    try {
      synchronized (this.executionDirDeletionSync) {
        if (!dir.exists()) {
          throw new ExecutorManagerException(
              "Execution dir file doesn't exist. Probably has beend deleted");
        }

        final File attachmentFile = runner.getJobAttachmentFile(jobId, attempt);
        if (attachmentFile == null || !attachmentFile.exists()) {
          return null;
        }

        final List<Object> jobAttachments =
            (ArrayList<Object>) JSONUtils.parseJSONFromFile(attachmentFile);

        return jobAttachments;
      }
    } catch (final IOException e) {
      throw new ExecutorManagerException(e);
    }
  }

  public JobMetaData readJobMetaData(final int execId, final String jobId, final int attempt,
      final int startByte, final int length) throws ExecutorManagerException {
    final FlowRunner runner = this.runningFlows.get(execId);
    if (runner == null) {
      throw new ExecutorManagerException("Running flow " + execId
          + " not found.");
    }

    final File dir = runner.getExecutionDir();
    if (dir != null && dir.exists()) {
      try {
        synchronized (this.executionDirDeletionSync) {
          if (!dir.exists()) {
            throw new ExecutorManagerException(
                "Execution dir file doesn't exist. Probably has beend deleted");
          }
          final File metaDataFile = runner.getJobMetaDataFile(jobId, attempt);
          if (metaDataFile != null && metaDataFile.exists()) {
            return FileIOUtils.readUtf8MetaDataFile(metaDataFile, startByte,
                length);
          } else {
            throw new ExecutorManagerException("Job log file doesn't exist.");
          }
        }
      } catch (final IOException e) {
        throw new ExecutorManagerException(e);
      }
    }

    throw new ExecutorManagerException(
        "Error reading file. Log directory doesn't exist.");
  }

  public long getLastCleanerThreadCheckTime() {
    return this.lastCleanerThreadCheckTime;
  }

  public boolean isCleanerThreadActive() {
    return this.cleanerThread.isAlive();
  }

  public State getCleanerThreadState() {
    return this.cleanerThread.getState();
  }

  public boolean isExecutorThreadPoolShutdown() {
    return this.executorService.isShutdown();
  }

  public int getNumQueuedFlows() {
    return this.executorService.getQueue().size();
  }

  public int getNumRunningFlows() {
    return this.executorService.getActiveCount();
  }

  public String getRunningFlowIds() {
    // The in progress tasks are actually of type FutureTask
    final Set<Runnable> inProgressTasks = this.executorService.getInProgressTasks();

    final List<Integer> runningFlowIds =
        new ArrayList<>(inProgressTasks.size());

    for (final Runnable task : inProgressTasks) {
      // add casting here to ensure it matches the expected type in
      // submittedFlows
      final Integer execId = this.submittedFlows.get((Future<?>) task);
      if (execId != null) {
        runningFlowIds.add(execId);
      } else {
        logger.warn("getRunningFlowIds: got null execId for task: " + task);
      }
    }

    Collections.sort(runningFlowIds);
    return runningFlowIds.toString();
  }

  public String getQueuedFlowIds() {
    final List<Integer> flowIdList =
        new ArrayList<>(this.executorService.getQueue().size());

    for (final Runnable task : this.executorService.getQueue()) {
      final Integer execId = this.submittedFlows.get(task);
      if (execId != null) {
        flowIdList.add(execId);
      } else {
        logger
            .warn("getQueuedFlowIds: got null execId for queuedTask: " + task);
      }
    }
    Collections.sort(flowIdList);
    return flowIdList.toString();
  }

  public int getMaxNumRunningFlows() {
    return this.numThreads;
  }

  public int getTheadPoolQueueSize() {
    return this.threadPoolQueueSize;
  }

  public void reloadJobTypePlugins() throws JobTypeManagerException {
    this.jobtypeManager.loadPlugins();
  }

  public int getTotalNumExecutedFlows() {
    return this.executorService.getTotalTasks();
  }

  @Override
  public void beforeExecute(final Runnable r) {
  }

  @Override
  public void afterExecute(final Runnable r) {
    this.submittedFlows.remove(r);
  }

  /**
   * This shuts down the flow runner. The call is blocking and awaits execution of all jobs.
   */
  public void shutdown() {
    logger.warn("Shutting down FlowRunnerManager...");
    if (this.azkabanProps.getBoolean(ConfigurationKeys.AZKABAN_POLL_MODEL, false)) {
      this.pollingService.shutdown();
    }
    this.executorService.shutdown();
    boolean result = false;
    while (!result) {
      logger.info("Awaiting Shutdown. # of executing flows: " + getNumRunningFlows());
      try {
        result = this.executorService.awaitTermination(1, TimeUnit.MINUTES);
      } catch (final InterruptedException e) {
        logger.error(e);
      }
    }
    logger.warn("Shutdown FlowRunnerManager complete.");
  }

  /**
   * This attempts shuts down the flow runner immediately (unsafe). This doesn't wait for jobs to
   * finish but interrupts all threads.
   */
  public void shutdownNow() {
    logger.warn("Shutting down FlowRunnerManager now...");
    if (this.azkabanProps.getBoolean(ConfigurationKeys.AZKABAN_POLL_MODEL, false)) {
      this.pollingService.shutdown();
    }
    this.executorService.shutdownNow();
    this.triggerManager.shutdown();
  }

  /**
   * Deleting old execution directory to free disk space.
   */
  public void deleteExecutionDirectory() {
    logger.warn("Deleting execution dir: " + this.executionDirectory.getAbsolutePath());
    try {
      FileUtils.deleteDirectory(this.executionDirectory);
    } catch (final IOException e) {
      logger.error(e);
    }
  }

  private class CleanerThread extends Thread {

    // Every 2 mins clean the recently finished list
    private static final long RECENTLY_FINISHED_INTERVAL_MS = 2 * 60 * 1000;
    // Every 5 mins kill flows running longer than allowed max running time
    private static final long LONG_RUNNING_FLOW_KILLING_INTERVAL_MS = 5 * 60 * 1000;
    private final long flowMaxRunningTimeInMins = FlowRunnerManager.this.azkabanProps.getInt(
        Constants.ConfigurationKeys.AZKABAN_MAX_FLOW_RUNNING_MINS, -1);
    private boolean shutdown = false;
    private long lastRecentlyFinishedCleanTime = -1;
    private long lastLongRunningFlowCleanTime = -1;

    public CleanerThread() {
      this.setName("FlowRunnerManager-Cleaner-Thread");
      setDaemon(true);
    }

    public void shutdown() {
      this.shutdown = true;
      this.interrupt();
    }

    private boolean isFlowRunningLongerThan(final ExecutableFlow flow,
        final long flowMaxRunningTimeInMins) {
      final Set<Status> nonFinishingStatusAfterFlowStarts = new HashSet<>(
          Arrays.asList(Status.RUNNING, Status.QUEUED, Status.PAUSED, Status.FAILED_FINISHING));
      return nonFinishingStatusAfterFlowStarts.contains(flow.getStatus()) && flow.getStartTime() > 0
          && TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - flow.getStartTime())
          >= flowMaxRunningTimeInMins;
    }

    @Override
    public void run() {
      while (!this.shutdown) {
        synchronized (this) {
          try {
            FlowRunnerManager.this.lastCleanerThreadCheckTime = System.currentTimeMillis();
            FlowRunnerManager.logger.info("# of executing flows: " + getNumRunningFlows());

            // Cleanup old stuff.
            final long currentTime = System.currentTimeMillis();
            if (currentTime - RECENTLY_FINISHED_INTERVAL_MS > this.lastRecentlyFinishedCleanTime) {
              FlowRunnerManager.logger.info("Cleaning recently finished");
              cleanRecentlyFinished();
              this.lastRecentlyFinishedCleanTime = currentTime;
            }

            if (this.flowMaxRunningTimeInMins > 0
                && currentTime - LONG_RUNNING_FLOW_KILLING_INTERVAL_MS
                > this.lastLongRunningFlowCleanTime) {
              FlowRunnerManager.logger
                  .info(String.format("Killing long jobs running longer than %s mins",
                      this.flowMaxRunningTimeInMins));
              for (final FlowRunner flowRunner : FlowRunnerManager.this.runningFlows.values()) {
                if (isFlowRunningLongerThan(flowRunner.getExecutableFlow(),
                    this.flowMaxRunningTimeInMins)) {
                  FlowRunnerManager.logger.info(String
                      .format("Killing job [id: %s, status: %s]. It has been running for %s mins",
                          flowRunner.getExecutableFlow().getId(),
                          flowRunner.getExecutableFlow().getStatus(), TimeUnit.MILLISECONDS
                              .toMinutes(System.currentTimeMillis() - flowRunner.getExecutableFlow()
                                  .getStartTime())));
                  flowRunner.kill();
                }
              }
              this.lastLongRunningFlowCleanTime = currentTime;
            }

            wait(FlowRunnerManager.RECENTLY_FINISHED_TIME_TO_LIVE);
          } catch (final InterruptedException e) {
            FlowRunnerManager.logger.info("Interrupted. Probably to shut down.");
          } catch (final Throwable t) {
            FlowRunnerManager.logger.warn(
                "Uncaught throwable, please look into why it is not caught", t);
          }
        }
      }
    }

    private void cleanRecentlyFinished() {
      final long cleanupThreshold =
          System.currentTimeMillis() - FlowRunnerManager.RECENTLY_FINISHED_TIME_TO_LIVE;
      final ArrayList<Integer> executionToKill = new ArrayList<>();
      for (final ExecutableFlow flow : FlowRunnerManager.this.recentlyFinishedFlows.values()) {
        if (flow.getEndTime() < cleanupThreshold) {
          executionToKill.add(flow.getExecutionId());
        }
      }

      for (final Integer id : executionToKill) {
        FlowRunnerManager.logger.info("Cleaning execution " + id
            + " from recently finished flows list.");
        FlowRunnerManager.this.recentlyFinishedFlows.remove(id);
      }
    }
  }

  /**
   * Polls new executions from DB periodically and submits the executions to run on the executor.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private class PollingService {

    private final ScheduledExecutorService scheduler;
    private final PollingCriteria pollingCriteria;
    private final long pollingIntervalMs;
    private int executorId = -1;
    private int numRetries = 0;

    public PollingService(final long pollingIntervalMs, final PollingCriteria pollingCriteria) {
      this.pollingIntervalMs = pollingIntervalMs;
      this.scheduler = Executors.newSingleThreadScheduledExecutor();
      this.pollingCriteria = pollingCriteria;
    }

    public void start() {
      this.scheduler.scheduleAtFixedRate(() -> pollExecution(), 0L, this.pollingIntervalMs,
          TimeUnit.MILLISECONDS);
    }

    private void pollExecution() {
      if (this.executorId == -1) {
        if (AzkabanExecutorServer.getApp() != null) {
          try {
            final Executor executor = requireNonNull(FlowRunnerManager.this.executorLoader
                .fetchExecutor(AzkabanExecutorServer.getApp().getHost(),
                    AzkabanExecutorServer.getApp().getPort()), "The executor can not be null");
            this.executorId = executor.getId();
          } catch (final Exception e) {
            FlowRunnerManager.logger.error("Failed to fetch executor ", e);
          }
        }
      } else if (this.pollingCriteria.shouldPoll()) {
        try {
          final int execId = FlowRunnerManager.this.executorLoader
              .selectAndUpdateExecution(this.executorId, FlowRunnerManager.this.active);
          if (execId != -1) {
            FlowRunnerManager.logger.info("Submitting flow " + execId);
            try {
              submitFlow(execId);
              FlowRunnerManager.this.commonMetrics.markDispatchSuccess();
              this.numRetries = 0;
            } catch (final ExecutorManagerException e) {
              // If the flow fails to be submitted, then unset its executor id in DB so that other
              // executors can pick up this flow and submit again.
              FlowRunnerManager.this.executorLoader.unsetExecutorIdForExecution(execId);
              throw new ExecutorManagerException(
                  "Unset executor id " + this.executorId + " for execution " + execId, e);
            }
          }
        } catch (final Exception e) {
          FlowRunnerManager.logger.error("Failed to submit flow ", e);
          FlowRunnerManager.this.commonMetrics.markDispatchFail();
          this.numRetries = this.numRetries + 1;
          try {
            // Implement exponential backoff retries when flow submission fails,
            // i.e., sleep 1s, 2s, 4s, 8s ... before next retries.
            Thread.sleep((long) (Math.pow(2, this.numRetries) * 1000));
          } catch (final InterruptedException ie) {
            FlowRunnerManager.logger
                .warn("Sleep after flow submission failure was interrupted - ignoring");
          }
        }
      }
    }

    public void shutdown() {
      this.scheduler.shutdown();
      this.scheduler.shutdownNow();
    }
  }

  private class PollingCriteria {

    private final Props azkabanProps;
    private final SystemMemoryInfo memInfo = SERVICE_PROVIDER.getInstance(SystemMemoryInfo.class);
    private final OsCpuUtil cpuUtil = SERVICE_PROVIDER.getInstance(OsCpuUtil.class);

    private boolean areFlowThreadsAvailable;
    private boolean isFreeMemoryAvailable;
    private boolean isCpuLoadUnderMax;

    public PollingCriteria(final Props azkabanProps) {
      this.azkabanProps = azkabanProps;
    }

    public boolean shouldPoll() {
      if (satisfiesFlowThreadsAvailableCriteria() && satisfiesFreeMemoryCriteria()
          && satisfiesCpuUtilizationCriteria()) {
        return true;
      }
      return false;
    }

    private boolean satisfiesFlowThreadsAvailableCriteria() {
      final boolean flowThreadsAvailableConfig = this.azkabanProps.
          getBoolean(ConfigurationKeys.AZKABAN_POLLING_CRITERIA_FLOW_THREADS_AVAILABLE, false);

      // allow polling if not present or configured with invalid value
      if (!flowThreadsAvailableConfig) {
        return true;
      }
      final int remainingFlowThreads = FlowRunnerManager.this.getMaxNumRunningFlows() -
          FlowRunnerManager.this.getNumRunningFlows();
      final boolean flowThreadsAvailable = remainingFlowThreads > 0;
      if (this.areFlowThreadsAvailable != flowThreadsAvailable) {
        this.areFlowThreadsAvailable = flowThreadsAvailable;
        if (flowThreadsAvailable) {
          FlowRunnerManager.logger.info("Polling criteria satisfied: available flow threads (" +
              remainingFlowThreads + ").");
        } else {
          FlowRunnerManager.logger.info("Polling criteria NOT satisfied: available flow threads (" +
              remainingFlowThreads + ").");
        }
      }

      return flowThreadsAvailable;
    }

    private boolean satisfiesFreeMemoryCriteria() {
      final int minFreeMemoryConfigGb = this.azkabanProps.
          getInt(ConfigurationKeys.AZKABAN_POLLING_CRITERIA_MIN_FREE_MEMORY_GB, 0);

      // allow polling if not present or configured with invalid value
      if (minFreeMemoryConfigGb <= 0) {
        return true;
      }
      final int minFreeMemoryConfigKb = minFreeMemoryConfigGb * 1024 * 1024;
      final boolean haveEnoughMemory =
          this.memInfo.isFreePhysicalMemoryAbove(minFreeMemoryConfigKb);
      if (this.isFreeMemoryAvailable != haveEnoughMemory) {
        this.isFreeMemoryAvailable = haveEnoughMemory;
        if (haveEnoughMemory) {
          FlowRunnerManager.logger.info("Polling criteria satisfied: available free memory.");
        } else {
          FlowRunnerManager.logger.info("Polling criteria NOT satisfied: available free memory.");
        }
      }

      return haveEnoughMemory;
    }

    private boolean satisfiesCpuUtilizationCriteria() {
      final double maxCpuUtilizationConfig = this.azkabanProps.
          getDouble(ConfigurationKeys.AZKABAN_POLLING_CRITERIA_MAX_CPU_UTILIZATION_PCT, 100);

      // allow polling if criteria not present or configured with invalid value
      if (maxCpuUtilizationConfig <= 0 || maxCpuUtilizationConfig >= 100) {
        return true;
      }

      final double cpuLoad = this.cpuUtil.getCpuLoad();
      if (cpuLoad == -1) {
        return true;
      }
      final boolean cpuLoadWithinParams = cpuLoad < maxCpuUtilizationConfig;
      if (this.isCpuLoadUnderMax != cpuLoadWithinParams) {
        this.isCpuLoadUnderMax = cpuLoadWithinParams;
        if (cpuLoadWithinParams) {
          FlowRunnerManager.logger.info("Polling criteria satisfied: Cpu utilization (" +
              cpuLoad + "%).");
        } else {
          FlowRunnerManager.logger.info("Polling criteria NOT satisfied: Cpu utilization (" +
              cpuLoad + "%).");
        }
      }

      return cpuLoadWithinParams;
    }

  }
}

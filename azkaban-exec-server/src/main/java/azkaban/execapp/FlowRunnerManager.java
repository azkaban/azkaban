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

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.LocalFlowWatcher;
import azkaban.execapp.event.RemoteFlowWatcher;
import azkaban.execapp.metric.NumFailedFlowMetric;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.jobtype.JobTypeManager;
import azkaban.jobtype.JobTypeManagerException;
import azkaban.metric.MetricReportManager;
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
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.ThreadPoolExecutingListener;
import azkaban.utils.TrackingThreadPool;
import azkaban.utils.UndefinedPropertyException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.Thread.State;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Execution manager for the server side execution.
 *
 * When a flow is submitted to FlowRunnerManager, it is the {@link Status.PREPARING} status. When a
 * flow is about to be executed by FlowRunner, its status is updated to {@link Status.RUNNING}
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
  private final Map<Integer, ExecutableFlow> recentlyFinishedFlows = new ConcurrentHashMap<>();
  private final TrackingThreadPool executorService;
  private final CleanerThread cleanerThread;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final JobTypeManager jobtypeManager;
  private final FlowPreparer flowPreparer;
  private final TriggerManager triggerManager;
  private final AzkabanEventReporter azkabanEventReporter;
  private final Props azkabanProps;
  private final File executionDirectory;
  private final File projectDirectory;
  private final Object executionDirDeletionSync = new Object();

  private Map<Pair<Integer, Integer>, ProjectVersion> installedProjects;
  private int numThreads = DEFAULT_NUM_EXECUTING_FLOWS;
  private int threadPoolQueueSize = -1;
  private int numJobThreadPerFlow = DEFAULT_FLOW_NUM_JOB_TREADS;
  private Props globalProps;
  private long lastCleanerThreadCheckTime = -1;
  private long executionDirRetention = 1 * 24 * 60 * 60 * 1000; // 1 Day
  // We want to limit the log sizes to about 20 megs
  private String jobLogChunkSize = "5MB";
  private int jobLogNumFiles = 4;
  // If true, jobs will validate proxy user against a list of valid proxy users.
  private boolean validateProxyUser = false;
  // date time of the the last flow submitted.
  private long lastFlowSubmittedDate = 0;
  // whether the current executor is active
  private volatile boolean isExecutorActive = false;

  @Inject
  public FlowRunnerManager(final Props props,
      final ExecutorLoader executorLoader,
      final ProjectLoader projectLoader,
      final StorageManager storageManager,
      final TriggerManager triggerManager,
      @Nullable final AzkabanEventReporter azkabanEventReporter) throws IOException {
    this.azkabanProps = props;

    this.executionDirRetention = props.getLong("execution.dir.retention",
        this.executionDirRetention);
    this.azkabanEventReporter = azkabanEventReporter;
    logger.info("Execution dir retention set to " + this.executionDirRetention + " ms");

    this.executionDirectory = new File(props.getString("azkaban.execution.dir", "executions"));
    if (!this.executionDirectory.exists()) {
      this.executionDirectory.mkdirs();
      setgidPermissionOnExecutionDirectory();
    }
    this.projectDirectory = new File(props.getString("azkaban.project.dir", "projects"));
    if (!this.projectDirectory.exists()) {
      this.projectDirectory.mkdirs();
    }

    this.installedProjects = new ConcurrentHashMap<>();

    // azkaban.temp.dir
    this.numThreads = props.getInt(EXECUTOR_FLOW_THREADS, DEFAULT_NUM_EXECUTING_FLOWS);
    this.numJobThreadPerFlow = props.getInt(FLOW_NUM_JOB_THREADS, DEFAULT_FLOW_NUM_JOB_TREADS);
    this.executorService = createExecutorService(this.numThreads);

    this.executorLoader = executorLoader;
    this.projectLoader = projectLoader;
    this.triggerManager = triggerManager;

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

    Long projectDirMaxSize = null;
    try {
      projectDirMaxSize = props.getLong(ConfigurationKeys.PROJECT_DIR_MAX_SIZE_IN_MB);
    } catch (final UndefinedPropertyException ex) {
    }

    // Create a flow preparer
    this.flowPreparer = new FlowPreparer(storageManager, this.executionDirectory,
        this.projectDirectory,
        this.installedProjects, this.runningFlows, projectDirMaxSize);

    this.cleanerThread = new CleanerThread();
    this.cleanerThread.start();
  }

  /*
   * Delete the project dir associated with {@code version}.
   * It first acquires object lock of {@code version} waiting for other threads creating
   * execution dir to finish to avoid race condition. An example of race condition scenario:
   * delete the dir of a project while an execution of a flow in the same project is being setup
   * and the flow's execution dir is being created({@link FlowPreparer#setup}).
   */
  static void deleteDirectory(final ProjectVersion pv) throws IOException {
    synchronized (pv) {
      logger.warn("Deleting project: " + pv);
      final File installedDir = pv.getInstalledDir();
      if (installedDir != null && installedDir.exists()) {
        FileUtils.deleteDirectory(installedDir);
      }
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

  private List<Path> loadExistingProjects() {
    final List<Path> projects = new ArrayList<>();
    for (final File project : this.projectDirectory.listFiles(new FilenameFilter() {

      String pattern = "[0-9]+\\.[0-9]+";

      @Override
      public boolean accept(final File dir, final String name) {
        return name.matches(this.pattern);
      }
    })) {
      if (project.isDirectory()) {
        projects.add(project.toPath());
      }
    }
    return projects;
  }

  private Map<Pair<Integer, Integer>, ProjectVersion> loadExistingProjectsAsCache() {
    final Map<Pair<Integer, Integer>, ProjectVersion> allProjects =
        new ConcurrentHashMap<>();
    for (final Path project : this.loadExistingProjects()) {
      if (Files.isDirectory(project)) {
        try {
          final String fileName = project.getFileName().toString();
          final int projectId = Integer.parseInt(fileName.split("\\.")[0]);
          final int versionNum = Integer.parseInt(fileName.split("\\.")[1]);
          final ProjectVersion version =
              new ProjectVersion(projectId, versionNum, project.toFile());

          version.setDirSizeInBytes(
              FileIOUtils.readNumberFromFile(Paths.get(version.getInstalledDir().toString(),
                  FlowPreparer.PROJECT_DIR_SIZE_FILE_NAME)));

          allProjects.put(new Pair<>(projectId, versionNum), version);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }

    return allProjects;
  }

  // todo chengren311: this method will be invoked by executor activate API, but in SOLO mode
  // the API is not called. So we should either have everything run in "multi-executor" mode
  // or make SOLO server mode call the API.
  public void setExecutorActive(final boolean isActive) {
    this.isExecutorActive = isActive;
    if (this.isExecutorActive) {
      this.installedProjects = this.loadExistingProjectsAsCache();
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
    // Load file and submit
    if (this.runningFlows.containsKey(execId)) {
      throw new ExecutorManagerException("Execution " + execId
          + " is already running.");
    }

    ExecutableFlow flow = null;
    flow = this.executorLoader.fetchExecutableFlow(execId);
    if (flow == null) {
      throw new ExecutorManagerException("Error loading flow with exec "
          + execId);
    }

    // Sets up the project files and execution directory.
    this.flowPreparer.setup(flow);

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
            this.azkabanProps, this.azkabanEventReporter);
    runner.setFlowWatcher(watcher)
        .setJobLogSettings(this.jobLogChunkSize, this.jobLogNumFiles)
        .setValidateProxyUser(this.validateProxyUser)
        .setNumJobThreads(numJobThreads).addListener(this);

    configureFlowLevelMetrics(runner);

    // Check again.
    if (this.runningFlows.containsKey(execId)) {
      throw new ExecutorManagerException("Execution " + execId
          + " is already running.");
    }

    // Finally, queue the sucker.
    this.runningFlows.put(execId, runner);

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
      } else if (event.getType() == EventType.FLOW_STARTED) {
        // add flow level SLA checker
        this.triggerManager
            .addTrigger(flow.getExecutionId(), SlaOption.getFlowLevelSLAOptions(flow));
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

  private Set<Pair<Integer, Integer>> getActiveProjectVersions() {
    final Set<Pair<Integer, Integer>> activeProjectVersions = new HashSet<>();
    for (final FlowRunner runner : FlowRunnerManager.this.runningFlows.values()) {
      final ExecutableFlow flow = runner.getExecutableFlow();
      activeProjectVersions.add(new Pair<>(flow
          .getProjectId(), flow.getVersion()));
    }
    return activeProjectVersions;
  }

  /**
   * Checks if the project version contains any running flow
   */
  private boolean isActiveProject(final ProjectVersion version) {
    final Pair<Integer, Integer> versionKey = new Pair<>(version.getProjectId(),
        version.getVersion());
    return getActiveProjectVersions().contains(versionKey);
  }


  private class CleanerThread extends Thread {

    // Every hour, clean execution dir.
    private static final long EXECUTION_DIR_CLEAN_INTERVAL_MS = 60 * 60 * 1000;
    // Every 5 mins clean the old project dir
    private static final long OLD_PROJECT_DIR_INTERVAL_MS = 5 * 60 * 1000;
    // Every 2 mins clean the recently finished list
    private static final long RECENTLY_FINISHED_INTERVAL_MS = 2 * 60 * 1000;
    // Every 5 mins kill flows running longer than allowed max running time
    private static final long LONG_RUNNING_FLOW_KILLING_INTERVAL_MS = 5 * 60 * 1000;
    private final long flowMaxRunningTimeInMins = FlowRunnerManager.this.azkabanProps.getInt(
        Constants.ConfigurationKeys.AZKABAN_MAX_FLOW_RUNNING_MINS, -1);
    private boolean shutdown = false;
    private long lastExecutionDirCleanTime = -1;
    private long lastOldProjectCleanTime = -1;
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
            logger.info("# of executing flows: " + getNumRunningFlows());

            // Cleanup old stuff.
            final long currentTime = System.currentTimeMillis();
            if (currentTime - RECENTLY_FINISHED_INTERVAL_MS > this.lastRecentlyFinishedCleanTime) {
              logger.info("Cleaning recently finished");
              cleanRecentlyFinished();
              this.lastRecentlyFinishedCleanTime = currentTime;
            }

            if (currentTime - OLD_PROJECT_DIR_INTERVAL_MS > this.lastOldProjectCleanTime
                && FlowRunnerManager.this.isExecutorActive) {
              logger.info("Cleaning old projects");
              cleanProjectsOfOldVersion();
              this.lastOldProjectCleanTime = currentTime;
            }

            if (currentTime - EXECUTION_DIR_CLEAN_INTERVAL_MS > this.lastExecutionDirCleanTime) {
              logger.info("Cleaning old execution dirs");
              cleanOlderExecutionDirs();
              this.lastExecutionDirCleanTime = currentTime;
            }

            if (this.flowMaxRunningTimeInMins > 0
                && currentTime - LONG_RUNNING_FLOW_KILLING_INTERVAL_MS
                > this.lastLongRunningFlowCleanTime) {
              logger.info(String.format("Killing long jobs running longer than %s mins",
                  this.flowMaxRunningTimeInMins));
              for (final FlowRunner flowRunner : FlowRunnerManager.this.runningFlows.values()) {
                if (isFlowRunningLongerThan(flowRunner.getExecutableFlow(),
                    this.flowMaxRunningTimeInMins)) {
                  logger.info(String
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

            wait(RECENTLY_FINISHED_TIME_TO_LIVE);
          } catch (final InterruptedException e) {
            logger.info("Interrupted. Probably to shut down.");
          } catch (final Throwable t) {
            logger.warn(
                "Uncaught throwable, please look into why it is not caught", t);
          }
        }
      }
    }

    private void cleanOlderExecutionDirs() {
      final File dir = FlowRunnerManager.this.executionDirectory;

      final long pastTimeThreshold =
          System.currentTimeMillis() - FlowRunnerManager.this.executionDirRetention;
      final File[] executionDirs = dir
          .listFiles(path -> path.isDirectory() && path.lastModified() < pastTimeThreshold);

      for (final File exDir : executionDirs) {
        try {
          final int execId = Integer.valueOf(exDir.getName());
          if (FlowRunnerManager.this.runningFlows.containsKey(execId)
              || FlowRunnerManager.this.recentlyFinishedFlows.containsKey(execId)) {
            continue;
          }
        } catch (final NumberFormatException e) {
          logger.error("Can't delete exec dir " + exDir.getName()
              + " it is not a number");
          continue;
        }

        synchronized (FlowRunnerManager.this.executionDirDeletionSync) {
          try {
            FileUtils.deleteDirectory(exDir);
          } catch (final IOException e) {
            logger.error("Error cleaning execution dir " + exDir.getPath(), e);
          }
        }
      }
    }

    private void cleanRecentlyFinished() {
      final long cleanupThreshold =
          System.currentTimeMillis() - RECENTLY_FINISHED_TIME_TO_LIVE;
      final ArrayList<Integer> executionToKill = new ArrayList<>();
      for (final ExecutableFlow flow : FlowRunnerManager.this.recentlyFinishedFlows.values()) {
        if (flow.getEndTime() < cleanupThreshold) {
          executionToKill.add(flow.getExecutionId());
        }
      }

      for (final Integer id : executionToKill) {
        logger.info("Cleaning execution " + id
            + " from recently finished flows list.");
        FlowRunnerManager.this.recentlyFinishedFlows.remove(id);
      }
    }

    private void cleanProjectsOfOldVersion() {
      final Map<Integer, ArrayList<ProjectVersion>> projectVersions =
          new HashMap<>();
      for (final ProjectVersion version : FlowRunnerManager.this.installedProjects.values()) {
        ArrayList<ProjectVersion> versionList =
            projectVersions.get(version.getProjectId());
        if (versionList == null) {
          versionList = new ArrayList<>();
          projectVersions.put(version.getProjectId(), versionList);
        }
        versionList.add(version);
      }

      for (final Map.Entry<Integer, ArrayList<ProjectVersion>> entry : projectVersions
          .entrySet()) {
        // Integer projectId = entry.getKey();
        final ArrayList<ProjectVersion> installedVersions = entry.getValue();

        // Keep one version of the project around.
        if (installedVersions.size() == 1) {
          continue;
        }

        Collections.sort(installedVersions);
        for (int i = 0; i < installedVersions.size() - 1; ++i) {
          final ProjectVersion version = installedVersions.get(i);
          if (!isActiveProject(version)) {
            try {
              logger.info("Removing old unused installed project "
                  + version.getProjectId() + ":" + version.getVersion());
              deleteDirectory(version);
              FlowRunnerManager.this.installedProjects.remove(new Pair<>(version
                  .getProjectId(), version.getVersion()));
            } catch (final IOException e) {
              logger.error(e);
            }
          }
        }
      }
    }

  }


}

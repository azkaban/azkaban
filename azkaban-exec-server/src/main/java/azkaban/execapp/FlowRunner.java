/*
 * Copyright 2013 LinkedIn Corp
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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_HOST_NAME;
import static azkaban.execapp.ConditionalWorkflowUtils.FAILED;
import static azkaban.execapp.ConditionalWorkflowUtils.PENDING;
import static azkaban.execapp.ConditionalWorkflowUtils.checkConditionOnJobStatus;
import static azkaban.project.DirectoryYamlFlowLoader.CONDITION_VARIABLE_REPLACEMENT_PATTERN;

import azkaban.Constants;
import azkaban.Constants.JobProperties;
import azkaban.ServiceProvider;
import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventHandler;
import azkaban.event.EventListener;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.JobCallbackManager;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.execapp.metric.NumFailedJobMetric;
import azkaban.execapp.metric.NumRunningJobMetric;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.ConditionOnJobStatus;
import azkaban.flow.FlowProps;
import azkaban.flow.FlowUtils;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobtype.JobTypeManager;
import azkaban.metric.MetricReportManager;
import azkaban.project.FlowLoaderUtils;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.sla.SlaOption;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import azkaban.utils.SwapQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Matcher;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


/**
 * Class that handles the running of a ExecutableFlow DAG
 */
public class FlowRunner extends EventHandler implements Runnable {

  private static final Layout DEFAULT_LAYOUT = new PatternLayout(
      "%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");
  // We check update every 5 minutes, just in case things get stuck. But for the
  // most part, we'll be idling.
  private static final long CHECK_WAIT_MS = 5 * 60 * 1000;
  private final ExecutableFlow flow;
  // Sync object for queuing
  private final Object mainSyncObj = new Object();
  private final JobTypeManager jobtypeManager;
  private final Layout loggerLayout = DEFAULT_LAYOUT;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final int execId;
  private final File execDir;
  private final ExecutionOptions.FailureAction failureAction;
  // Properties map
  private final Props azkabanProps;
  private final Map<String, Props> sharedProps = new HashMap<>();
  private final JobRunnerEventListener listener = new JobRunnerEventListener();
  private final FlowRunnerEventListener flowListener = new FlowRunnerEventListener();
  private final Set<JobRunner> activeJobRunners = Collections
      .newSetFromMap(new ConcurrentHashMap<JobRunner, Boolean>());
  // Thread safe swap queue for finishedExecutions.
  private final SwapQueue<ExecutableNode> finishedNodes;
  private final AzkabanEventReporter azkabanEventReporter;
  private Logger logger;
  private Appender flowAppender;
  private File logFile;
  private ExecutorService executorService;
  private Thread flowRunnerThread;
  private int numJobThreads = 10;
  // Used for pipelining
  private Integer pipelineLevel = null;
  private Integer pipelineExecId = null;
  // Watches external flows for execution.
  private FlowWatcher watcher = null;
  private Set<String> proxyUsers = null;
  private boolean validateUserProxy;
  private String jobLogFileSize = "5MB";
  private int jobLogNumFiles = 4;
  private volatile boolean flowPaused = false;
  private volatile boolean flowFailed = false;
  private volatile boolean flowFinished = false;
  private volatile boolean flowKilled = false;

  // The following is state that will trigger a retry of all failed jobs
  private volatile boolean retryFailedJobs = false;

  /**
   * Constructor. This will create its own ExecutorService for thread pools
   */
  public FlowRunner(final ExecutableFlow flow, final ExecutorLoader executorLoader,
      final ProjectLoader projectLoader, final JobTypeManager jobtypeManager,
      final Props azkabanProps, final AzkabanEventReporter azkabanEventReporter)
      throws ExecutorManagerException {
    this(flow, executorLoader, projectLoader, jobtypeManager, null, azkabanProps,
        azkabanEventReporter);
  }

  /**
   * Constructor. If executorService is null, then it will create it's own for thread pools.
   */
  public FlowRunner(final ExecutableFlow flow, final ExecutorLoader executorLoader,
      final ProjectLoader projectLoader, final JobTypeManager jobtypeManager,
      final ExecutorService executorService, final Props azkabanProps,
      final AzkabanEventReporter azkabanEventReporter)
      throws ExecutorManagerException {
    this.execId = flow.getExecutionId();
    this.flow = flow;
    this.executorLoader = executorLoader;
    this.projectLoader = projectLoader;
    this.execDir = new File(flow.getExecutionPath());
    this.jobtypeManager = jobtypeManager;

    final ExecutionOptions options = flow.getExecutionOptions();
    this.pipelineLevel = options.getPipelineLevel();
    this.pipelineExecId = options.getPipelineExecutionId();
    this.failureAction = options.getFailureAction();
    this.proxyUsers = flow.getProxyUsers();
    this.executorService = executorService;
    this.finishedNodes = new SwapQueue<>();
    this.azkabanProps = azkabanProps;
    // Add the flow listener only if a non-null eventReporter is available.
    if (azkabanEventReporter != null) {
      this.addListener(this.flowListener);
    }

    // Create logger and execution dir in flowRunner initialization instead of flow runtime to avoid NPE
    // where the uninitialized logger is used in flow preparing state
    createLogger(this.flow.getFlowId());
    this.azkabanEventReporter = azkabanEventReporter;
  }

  public FlowRunner setFlowWatcher(final FlowWatcher watcher) {
    this.watcher = watcher;
    return this;
  }

  public FlowRunner setNumJobThreads(final int jobs) {
    this.numJobThreads = jobs;
    return this;
  }

  public FlowRunner setJobLogSettings(final String jobLogFileSize, final int jobLogNumFiles) {
    this.jobLogFileSize = jobLogFileSize;
    this.jobLogNumFiles = jobLogNumFiles;

    return this;
  }

  public FlowRunner setValidateProxyUser(final boolean validateUserProxy) {
    this.validateUserProxy = validateUserProxy;
    return this;
  }

  public File getExecutionDir() {
    return this.execDir;
  }

  @Override
  public void run() {
    try {
      if (this.executorService == null) {
        this.executorService = Executors.newFixedThreadPool(this.numJobThreads);
      }
      setupFlowExecution();
      this.flow.setStartTime(System.currentTimeMillis());

      this.logger.info("Updating initial flow directory.");
      updateFlow();
      this.logger.info("Fetching job and shared properties.");
      if (!FlowLoaderUtils.isAzkabanFlowVersion20(this.flow.getAzkabanFlowVersion())) {
        loadAllProperties();
      }

      this.fireEventListeners(
          Event.create(this, EventType.FLOW_STARTED, new EventData(this.getExecutableFlow())));
      runFlow();
    } catch (final Throwable t) {
      if (this.logger != null) {
        this.logger
            .error(
                "An error has occurred during the running of the flow. Quiting.",
                t);
      }
      this.flow.setStatus(Status.FAILED);
    } finally {
      try {
        if (this.watcher != null) {
          this.logger.info("Watcher is attached. Stopping watcher.");
          this.watcher.stopWatcher();
          this.logger
              .info("Watcher cancelled status is " + this.watcher.isWatchCancelled());
        }

        this.flow.setEndTime(System.currentTimeMillis());
        this.logger.info("Setting end time for flow " + this.execId + " to "
            + System.currentTimeMillis());
        closeLogger();
        updateFlow();
      } finally {
        this.fireEventListeners(
            Event.create(this, EventType.FLOW_FINISHED, new EventData(this.flow)));
      }
    }
  }

  private void setupFlowExecution() {
    final int projectId = this.flow.getProjectId();
    final int version = this.flow.getVersion();
    final String flowId = this.flow.getFlowId();

    // Add a bunch of common azkaban properties
    Props commonFlowProps = FlowUtils.addCommonFlowProperties(null, this.flow);

    if (FlowLoaderUtils.isAzkabanFlowVersion20(this.flow.getAzkabanFlowVersion())) {
      final Props flowProps = loadPropsFromYamlFile(this.flow.getId());
      if (flowProps != null) {
        flowProps.setParent(commonFlowProps);
        commonFlowProps = flowProps;
      }
    } else {
      if (this.flow.getJobSource() != null) {
        final String source = this.flow.getJobSource();
        final Props flowProps = this.sharedProps.get(source);
        flowProps.setParent(commonFlowProps);
        commonFlowProps = flowProps;
      }
    }

    // If there are flow overrides, we apply them now.
    final Map<String, String> flowParam =
        this.flow.getExecutionOptions().getFlowParameters();
    if (flowParam != null && !flowParam.isEmpty()) {
      commonFlowProps = new Props(commonFlowProps, flowParam);
    }
    this.flow.setInputProps(commonFlowProps);

    if (this.watcher != null) {
      this.watcher.setLogger(this.logger);
    }

    // Avoid NPE in unit tests when the static app instance is not set
    if (AzkabanExecutorServer.getApp() != null) {
      this.logger
          .info("Assigned executor : " + AzkabanExecutorServer.getApp().getExecutorHostPort());
    }
    this.logger.info("Running execid:" + this.execId + " flow:" + flowId + " project:"
        + projectId + " version:" + version);
    if (this.pipelineExecId != null) {
      this.logger.info("Running simulateously with " + this.pipelineExecId
          + ". Pipelining level " + this.pipelineLevel);
    }

    // The current thread is used for interrupting blocks
    this.flowRunnerThread = Thread.currentThread();
    this.flowRunnerThread.setName("FlowRunner-exec-" + this.flow.getExecutionId());
  }

  private void updateFlow() {
    updateFlow(System.currentTimeMillis());
  }

  private synchronized void updateFlow(final long time) {
    try {
      this.flow.setUpdateTime(time);
      this.executorLoader.updateExecutableFlow(this.flow);
    } catch (final ExecutorManagerException e) {
      this.logger.error("Error updating flow.", e);
    }
  }

  /**
   * setup logger and execution dir for the flowId
   */
  private void createLogger(final String flowId) {
    // Create logger
    final String loggerName = this.execId + "." + flowId;
    this.logger = Logger.getLogger(loggerName);

    // Create file appender
    final String logName = "_flow." + loggerName + ".log";
    this.logFile = new File(this.execDir, logName);
    final String absolutePath = this.logFile.getAbsolutePath();

    this.flowAppender = null;
    try {
      this.flowAppender = new FileAppender(this.loggerLayout, absolutePath, false);
      this.logger.addAppender(this.flowAppender);
    } catch (final IOException e) {
      this.logger.error("Could not open log file in " + this.execDir, e);
    }
  }

  private void closeLogger() {
    if (this.logger != null) {
      this.logger.removeAppender(this.flowAppender);
      this.flowAppender.close();

      try {
        this.executorLoader.uploadLogFile(this.execId, "", 0, this.logFile);
      } catch (final ExecutorManagerException e) {
        e.printStackTrace();
      }
    }
  }

  private void loadAllProperties() throws IOException {
    // First load all the properties
    for (final FlowProps fprops : this.flow.getFlowProps()) {
      final String source = fprops.getSource();
      final File propsPath = new File(this.execDir, source);
      final Props props = new Props(null, propsPath);
      this.sharedProps.put(source, props);
    }

    // Resolve parents
    for (final FlowProps fprops : this.flow.getFlowProps()) {
      if (fprops.getInheritedSource() != null) {
        final String source = fprops.getSource();
        final String inherit = fprops.getInheritedSource();

        final Props props = this.sharedProps.get(source);
        final Props inherits = this.sharedProps.get(inherit);

        props.setParent(inherits);
      }
    }
  }

  /**
   * Main method that executes the jobs.
   */
  private void runFlow() throws Exception {
    this.logger.info("Starting flows");
    runReadyJob(this.flow);
    updateFlow();

    while (!this.flowFinished) {
      synchronized (this.mainSyncObj) {
        if (this.flowPaused) {
          try {
            this.mainSyncObj.wait(CHECK_WAIT_MS);
          } catch (final InterruptedException e) {
          }

          continue;
        } else {
          if (this.retryFailedJobs) {
            retryAllFailures();
          } else if (!progressGraph()) {
            try {
              this.mainSyncObj.wait(CHECK_WAIT_MS);
            } catch (final InterruptedException e) {
            }
          }
        }
      }
    }

    this.logger.info("Finishing up flow. Awaiting Termination");
    this.executorService.shutdown();

    updateFlow();
    this.logger.info("Finished Flow");
  }

  private void retryAllFailures() throws IOException {
    this.logger.info("Restarting all failed jobs");

    this.retryFailedJobs = false;
    this.flowKilled = false;
    this.flowFailed = false;
    this.flow.setStatus(Status.RUNNING);

    final ArrayList<ExecutableNode> retryJobs = new ArrayList<>();
    resetFailedState(this.flow, retryJobs);

    for (final ExecutableNode node : retryJobs) {
      if (node.getStatus() == Status.READY
          || node.getStatus() == Status.DISABLED) {
        runReadyJob(node);
      } else if (node.getStatus() == Status.SUCCEEDED) {
        for (final String outNodeId : node.getOutNodes()) {
          final ExecutableFlowBase base = node.getParentFlow();
          runReadyJob(base.getExecutableNode(outNodeId));
        }
      }

      runReadyJob(node);
    }

    updateFlow();
  }

  private boolean progressGraph() throws IOException {
    this.finishedNodes.swap();

    // The following nodes are finished, so we'll collect a list of outnodes
    // that are candidates for running next.
    final HashSet<ExecutableNode> nodesToCheck = new HashSet<>();
    for (final ExecutableNode node : this.finishedNodes) {
      Set<String> outNodeIds = node.getOutNodes();
      ExecutableFlowBase parentFlow = node.getParentFlow();

      // If a job is seen as failed or killed due to failing SLA, then we set the parent flow to
      // FAILED_FINISHING
      if (node.getStatus() == Status.FAILED || (node.getStatus() == Status.KILLED && node
          .isKilledBySLA())) {
        // The job cannot be retried or has run out of retry attempts. We will
        // fail the job and its flow now.
        if (!retryJobIfPossible(node)) {
          setFlowFailed(node);
        } else {
          nodesToCheck.add(node);
          continue;
        }
      }

      if (outNodeIds.isEmpty() && isFlowReadytoFinalize(parentFlow)) {
        // Todo jamiesjc: For conditional workflows, if conditionOnJobStatus is ONE_SUCCESS or
        // ONE_FAILED, some jobs might still be running when the end nodes have finished. In this
        // case, we need to kill all running jobs before finalizing the flow.
        finalizeFlow(parentFlow);
        finishExecutableNode(parentFlow);

        // If the parent has a parent, then we process
        if (!(parentFlow instanceof ExecutableFlow)) {
          outNodeIds = parentFlow.getOutNodes();
          parentFlow = parentFlow.getParentFlow();
        }
      }

      // Add all out nodes from the finished job. We'll check against this set
      // to
      // see if any are candidates for running.
      for (final String nodeId : outNodeIds) {
        final ExecutableNode outNode = parentFlow.getExecutableNode(nodeId);
        nodesToCheck.add(outNode);
      }
    }

    // Runs candidate jobs. The code will check to see if they are ready to run
    // before
    // Instant kill or skip if necessary.
    boolean jobsRun = false;
    for (final ExecutableNode node : nodesToCheck) {
      if (notReadyToRun(node.getStatus())) {
        // Really shouldn't get in here.
        continue;
      }

      jobsRun |= runReadyJob(node);
    }

    if (jobsRun || this.finishedNodes.getSize() > 0) {
      updateFlow();
      return true;
    }

    return false;
  }

  private void setFlowFailed(final ExecutableNode node) {
    boolean shouldFail = true;
    // As long as there is no outNodes or at least one outNode has conditionOnJobStatus of
    // ALL_SUCCESS, we should set the flow to failed. Otherwise, it could still statisfy the
    // condition of conditional workflows, so don't set the flow to failed.
    for (final String outNodeId : node.getOutNodes()) {
      if (node.getParentFlow().getExecutableNode(outNodeId).getConditionOnJobStatus()
          .equals(ConditionOnJobStatus.ALL_SUCCESS)) {
        shouldFail = true;
        break;
      } else {
        shouldFail = false;
      }
    }

    if (shouldFail) {
      propagateStatus(node.getParentFlow(),
          node.getStatus() == Status.KILLED ? Status.KILLED : Status.FAILED_FINISHING);
      if (this.failureAction == FailureAction.CANCEL_ALL) {
        this.kill();
      }
      this.flowFailed = true;
    }
  }

  private boolean notReadyToRun(final Status status) {
    return Status.isStatusFinished(status)
        || Status.isStatusRunning(status)
        || Status.KILLING == status;
  }

  private boolean runReadyJob(final ExecutableNode node) throws IOException {
    if (Status.isStatusFinished(node.getStatus())
        || Status.isStatusRunning(node.getStatus())) {
      return false;
    }

    final Status nextNodeStatus = getImpliedStatus(node);
    if (nextNodeStatus == null) {
      return false;
    }

    if (nextNodeStatus == Status.CANCELLED) {
      this.logger.info("Cancelling '" + node.getNestedId()
          + "' due to prior errors.");
      node.cancelNode(System.currentTimeMillis());
      finishExecutableNode(node);
    } else if (nextNodeStatus == Status.SKIPPED) {
      this.logger.info("Skipping disabled job '" + node.getId() + "'.");
      node.skipNode(System.currentTimeMillis());
      finishExecutableNode(node);
    } else if (nextNodeStatus == Status.READY) {
      if (node instanceof ExecutableFlowBase) {
        final ExecutableFlowBase flow = ((ExecutableFlowBase) node);
        this.logger.info("Running flow '" + flow.getNestedId() + "'.");
        flow.setStatus(Status.RUNNING);
        flow.setStartTime(System.currentTimeMillis());
        prepareJobProperties(flow);

        for (final String startNodeId : ((ExecutableFlowBase) node).getStartNodes()) {
          final ExecutableNode startNode = flow.getExecutableNode(startNodeId);
          runReadyJob(startNode);
        }
      } else {
        runExecutableNode(node);
      }
    }
    return true;
  }

  private boolean retryJobIfPossible(final ExecutableNode node) {
    if (node instanceof ExecutableFlowBase) {
      return false;
    }

    if (node.getRetries() > node.getAttempt()) {
      this.logger.info("Job '" + node.getId() + "' will be retried. Attempt "
          + node.getAttempt() + " of " + node.getRetries());
      node.setDelayedExecution(node.getRetryBackoff());
      node.resetForRetry();
      return true;
    } else {
      if (node.getRetries() > 0) {
        this.logger.info("Job '" + node.getId() + "' has run out of retry attempts");
        // Setting delayed execution to 0 in case this is manually re-tried.
        node.setDelayedExecution(0);
      }

      return false;
    }
  }

  private void propagateStatus(final ExecutableFlowBase base, final Status status) {
    if (!Status.isStatusFinished(base.getStatus()) && base.getStatus() != Status.KILLING) {
      this.logger.info("Setting " + base.getNestedId() + " to " + status);
      base.setStatus(status);
      if (base.getParentFlow() != null) {
        propagateStatus(base.getParentFlow(), status);
      }
    }
  }

  private void finishExecutableNode(final ExecutableNode node) {
    this.finishedNodes.add(node);
    final EventData eventData = new EventData(node.getStatus(), node.getNestedId());
    fireEventListeners(Event.create(this, EventType.JOB_FINISHED, eventData));
  }

  private boolean isFlowReadytoFinalize(final ExecutableFlowBase flow) {
    // Only when all the end nodes are finished, the flow is ready to finalize.
    for (final String end : flow.getEndNodes()) {
      if (!Status.isStatusFinished(flow.getExecutableNode(end).getStatus())) {
        return false;
      }
    }
    return true;
  }

  private void finalizeFlow(final ExecutableFlowBase flow) {
    final String id = flow == this.flow ? "" : flow.getNestedId();

    // If it's not the starting flow, we'll create set of output props
    // for the finished flow.
    boolean succeeded = true;
    Props previousOutput = null;

    for (final String end : flow.getEndNodes()) {
      final ExecutableNode node = flow.getExecutableNode(end);

      if (node.getStatus() == Status.KILLED
          || node.getStatus() == Status.KILLING
          || node.getStatus() == Status.FAILED
          || node.getStatus() == Status.CANCELLED) {
        succeeded = false;
      }

      Props output = node.getOutputProps();
      if (output != null) {
        output = Props.clone(output);
        output.setParent(previousOutput);
        previousOutput = output;
      }
    }

    flow.setOutputProps(previousOutput);
    if (!succeeded && (flow.getStatus() == Status.RUNNING)) {
      flow.setStatus(Status.KILLED);
    }

    flow.setEndTime(System.currentTimeMillis());
    flow.setUpdateTime(System.currentTimeMillis());
    final long durationSec = (flow.getEndTime() - flow.getStartTime()) / 1000;
    switch (flow.getStatus()) {
      case FAILED_FINISHING:
        this.logger.info("Setting flow '" + id + "' status to FAILED in "
            + durationSec + " seconds");
        flow.setStatus(Status.FAILED);
        break;
      case KILLING:
        this.logger
            .info("Setting flow '" + id + "' status to KILLED in " + durationSec + " seconds");
        flow.setStatus(Status.KILLED);
        break;
      case FAILED:
      case KILLED:
      case CANCELLED:
      case FAILED_SUCCEEDED:
        this.logger.info("Flow '" + id + "' is set to " + flow.getStatus().toString()
            + " in " + durationSec + " seconds");
        break;
      default:
        flow.setStatus(Status.SUCCEEDED);
        this.logger.info("Flow '" + id + "' is set to " + flow.getStatus().toString()
            + " in " + durationSec + " seconds");
    }

    // If the finalized flow is actually the top level flow, than we finish
    // the main loop.
    if (flow instanceof ExecutableFlow) {
      this.flowFinished = true;
    }
  }

  private void prepareJobProperties(final ExecutableNode node) throws IOException {
    if (node instanceof ExecutableFlow) {
      return;
    }

    Props props = null;

    if (!FlowLoaderUtils.isAzkabanFlowVersion20(this.flow.getAzkabanFlowVersion())) {
      // 1. Shared properties (i.e. *.properties) for the jobs only. This takes
      // the
      // least precedence
      if (!(node instanceof ExecutableFlowBase)) {
        final String sharedProps = node.getPropsSource();
        if (sharedProps != null) {
          props = this.sharedProps.get(sharedProps);
        }
      }
    }

    // The following is the hiearchical ordering of dependency resolution
    // 2. Parent Flow Properties
    final ExecutableFlowBase parentFlow = node.getParentFlow();
    if (parentFlow != null) {
      final Props flowProps = Props.clone(parentFlow.getInputProps());
      flowProps.setEarliestAncestor(props);
      props = flowProps;
    }

    // 3. Output Properties. The call creates a clone, so we can overwrite it.
    final Props outputProps = collectOutputProps(node);
    if (outputProps != null) {
      outputProps.setEarliestAncestor(props);
      props = outputProps;
    }

    // 4. The job source.
    final Props jobSource = loadJobProps(node);
    if (jobSource != null) {
      jobSource.setParent(props);
      props = jobSource;
    }

    node.setInputProps(props);
  }

  /**
   * @param props This method is to put in any job properties customization before feeding to the
   * job.
   */
  private void customizeJobProperties(final Props props) {
    final boolean memoryCheck = this.flow.getExecutionOptions().getMemoryCheck();
    props.put(ProcessJob.AZKABAN_MEMORY_CHECK, Boolean.toString(memoryCheck));
  }

  private Props loadJobProps(final ExecutableNode node) throws IOException {
    Props props = null;
    if (FlowLoaderUtils.isAzkabanFlowVersion20(this.flow.getAzkabanFlowVersion())) {
      final String jobPath =
          node.getParentFlow().getFlowId() + Constants.PATH_DELIMITER + node.getId();
      props = loadPropsFromYamlFile(jobPath);
      if (props == null) {
        this.logger.info("Job props loaded from yaml file is empty for job " + node.getId());
        return props;
      }
    } else {
      final String source = node.getJobSource();
      if (source == null) {
        return null;
      }

      // load the override props if any
      try {
        props =
            this.projectLoader.fetchProjectProperty(this.flow.getProjectId(),
                this.flow.getVersion(), node.getId() + Constants.JOB_OVERRIDE_SUFFIX);
      } catch (final ProjectManagerException e) {
        e.printStackTrace();
        this.logger.error("Error loading job override property for job "
            + node.getId());
      }

      final File path = new File(this.execDir, source);
      if (props == null) {
        // if no override prop, load the original one on disk
        try {
          props = new Props(null, path);
        } catch (final IOException e) {
          e.printStackTrace();
          this.logger.error("Error loading job file " + source + " for job "
              + node.getId());
        }
      }
      // setting this fake source as this will be used to determine the location
      // of log files.
      if (path.getPath() != null) {
        props.setSource(path.getPath());
      }
    }

    customizeJobProperties(props);

    return props;
  }

  private Props loadPropsFromYamlFile(final String path) {
    File tempDir = null;
    Props props = null;
    try {
      tempDir = Files.createTempDir();
      props = FlowLoaderUtils.getPropsFromYamlFile(path, getFlowFile(tempDir));
    } catch (final Exception e) {
      this.logger.error("Failed to get props from flow file. " + e);
    } finally {
      if (tempDir != null && tempDir.exists()) {
        try {
          FileUtils.deleteDirectory(tempDir);
        } catch (final IOException e) {
          this.logger.error("Failed to delete temp directory." + e);
          tempDir.deleteOnExit();
        }
      }
    }
    return props;
  }

  private File getFlowFile(final File tempDir) throws Exception {
    final List<FlowProps> flowPropsList = ImmutableList.copyOf(this.flow.getFlowProps());
    // There should be exact one source (file name) for each flow file.
    if (flowPropsList.isEmpty() || flowPropsList.get(0) == null) {
      throw new ProjectManagerException(
          "Failed to get flow file source. Flow props is empty for " + this.flow.getId());
    }
    final String source = flowPropsList.get(0).getSource();
    final int flowVersion = this.projectLoader
        .getLatestFlowVersion(this.flow.getProjectId(), this.flow.getVersion(), source);
    final File flowFile = this.projectLoader
        .getUploadedFlowFile(this.flow.getProjectId(), this.flow.getVersion(), source,
            flowVersion, tempDir);

    return flowFile;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void runExecutableNode(final ExecutableNode node) throws IOException {
    // Collect output props from the job's dependencies.
    prepareJobProperties(node);

    node.setStatus(Status.QUEUED);
    final JobRunner runner = createJobRunner(node);
    this.logger.info("Submitting job '" + node.getNestedId() + "' to run.");
    try {
      this.executorService.submit(runner);
      this.activeJobRunners.add(runner);
    } catch (final RejectedExecutionException e) {
      this.logger.error(e);
    }
  }

  /**
   * Determines what the state of the next node should be. Returns null if the node should not be
   * run.
   */
  public Status getImpliedStatus(final ExecutableNode node) {
    // If it's running or finished with 'SUCCEEDED', than don't even
    // bother starting this job.
    if (Status.isStatusRunning(node.getStatus())
        || node.getStatus() == Status.SUCCEEDED) {
      return null;
    }

    // Go through the node's dependencies. If all of the previous job's
    // statuses is finished and not FAILED or KILLED, than we can safely
    // run this job.
    Status status = Status.READY;

    // Check if condition on job status is satisfied
    switch (checkConditionOnJobStatus(node)) {
      case FAILED:
        status = Status.CANCELLED;
        break;
      // Condition not satisfied yet, need to wait
      case PENDING:
        return null;
      default:
        break;
    }

    if (!isConditionOnRuntimeVariableMet(node)) {
      status = Status.CANCELLED;
    }

    // If it's disabled but ready to run, we want to make sure it continues
    // being disabled.
    if (node.getStatus() == Status.DISABLED
        || node.getStatus() == Status.SKIPPED) {
      return Status.SKIPPED;
    }

    // If the flow has failed, and we want to finish only the currently running
    // jobs, we just
    // kill everything else. We also kill, if the flow has been cancelled.
    if (this.flowFailed
        && this.failureAction == ExecutionOptions.FailureAction.FINISH_CURRENTLY_RUNNING) {
      return Status.CANCELLED;
    } else if (isKilled()) {
      return Status.CANCELLED;
    }

    return status;
  }

  private Boolean isConditionOnRuntimeVariableMet(final ExecutableNode node) {
    final String condition = node.getCondition();
    if (condition == null) {
      return true;
    }

    final Matcher matcher = CONDITION_VARIABLE_REPLACEMENT_PATTERN.matcher(condition);
    String replaced = condition;

    while (matcher.find()) {
      final String value = findValueForJobVariable(node, matcher.group(1), matcher.group(2));
      if (value != null) {
        replaced = replaced.replace(matcher.group(), "'" + value + "'");
      }
      this.logger.info("Condition is " + replaced);
    }

    // Evaluate string expression using script engine
    return evaluateExpression(replaced);
  }

  private String findValueForJobVariable(final ExecutableNode node, final String jobName, final
  String variable) {
    // Get job output props
    final ExecutableNode target = node.getParentFlow().getExecutableNode(jobName);
    if (target == null) {
      this.logger.error("Not able to load props from output props file, job name " + jobName
          + " might be invalid.");
      return null;
    }

    final Props outputProps = target.getOutputProps();
    if (outputProps != null && outputProps.containsKey(variable)) {
      return outputProps.get(variable);
    }

    return null;
  }

  private boolean evaluateExpression(final String expression) {
    boolean result = false;
    final ScriptEngineManager sem = new ScriptEngineManager();
    final ScriptEngine se = sem.getEngineByName("JavaScript");

    // Restrict permission using the two-argument form of doPrivileged()
    try {
      final Object object = AccessController.doPrivileged(
          new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws ScriptException {
              return se.eval(expression);
            }
          },
          new AccessControlContext(
              new ProtectionDomain[]{new ProtectionDomain(null, null)}) // no permissions
      );
      if (object != null) {
        result = (boolean) object;
      }
    } catch (final Exception e) {
      this.logger.error("Failed to evaluate the expression.", e);
    }

    this.logger.info("Evaluate expression result: " + result);
    return result;
  }

  private Props collectOutputProps(final ExecutableNode node) {
    Props previousOutput = null;
    // Iterate the in nodes again and create the dependencies
    for (final String dependency : node.getInNodes()) {
      Props output =
          node.getParentFlow().getExecutableNode(dependency).getOutputProps();
      if (output != null) {
        output = Props.clone(output);
        output.setParent(previousOutput);
        previousOutput = output;
      }
    }

    return previousOutput;
  }

  private JobRunner createJobRunner(final ExecutableNode node) {
    // Load job file.
    final File path = new File(this.execDir, node.getJobSource());

    final JobRunner jobRunner =
        new JobRunner(node, path.getParentFile(), this.executorLoader,
            this.jobtypeManager, this.azkabanProps);
    if (this.watcher != null) {
      jobRunner.setPipeline(this.watcher, this.pipelineLevel);
    }
    if (this.validateUserProxy) {
      jobRunner.setValidatedProxyUsers(this.proxyUsers);
    }

    jobRunner.setDelayStart(node.getDelayedExecution());
    jobRunner.setLogSettings(this.logger, this.jobLogFileSize, this.jobLogNumFiles);
    jobRunner.addListener(this.listener);

    if (JobCallbackManager.isInitialized()) {
      jobRunner.addListener(JobCallbackManager.getInstance());
    }

    configureJobLevelMetrics(jobRunner);

    return jobRunner;
  }

  /**
   * Configure Azkaban metrics tracking for a new jobRunner instance
   */
  private void configureJobLevelMetrics(final JobRunner jobRunner) {
    this.logger.info("Configuring Azkaban metrics tracking for jobrunner object");
    if (MetricReportManager.isAvailable()) {
      final MetricReportManager metricManager = MetricReportManager.getInstance();

      // Adding NumRunningJobMetric listener
      jobRunner.addListener((NumRunningJobMetric) metricManager
          .getMetricFromName(NumRunningJobMetric.NUM_RUNNING_JOB_METRIC_NAME));

      // Adding NumFailedJobMetric listener
      jobRunner.addListener((NumFailedJobMetric) metricManager
          .getMetricFromName(NumFailedJobMetric.NUM_FAILED_JOB_METRIC_NAME));

    }

    jobRunner.addListener(JmxJobMBeanManager.getInstance());
  }

  public void pause(final String user) {
    synchronized (this.mainSyncObj) {
      if (!this.flowFinished) {
        this.logger.info("Flow paused by " + user);
        this.flowPaused = true;
        this.flow.setStatus(Status.PAUSED);

        updateFlow();
      } else {
        this.logger.info("Cannot pause finished flow. Called by user " + user);
      }
    }

    interrupt();
  }

  public void resume(final String user) {
    synchronized (this.mainSyncObj) {
      if (!this.flowPaused) {
        this.logger.info("Cannot resume flow that isn't paused");
      } else {
        this.logger.info("Flow resumed by " + user);
        this.flowPaused = false;
        if (this.flowFailed) {
          this.flow.setStatus(Status.FAILED_FINISHING);
        } else if (this.flowKilled) {
          this.flow.setStatus(Status.KILLING);
        } else {
          this.flow.setStatus(Status.RUNNING);
        }

        updateFlow();
      }
    }

    interrupt();
  }

  public void kill(final String user) {
    this.logger.info("Flow killed by " + user);
    kill();
  }

  public void kill() {
    synchronized (this.mainSyncObj) {
      if (this.flowKilled) {
        return;
      }
      this.logger.info("Kill has been called on flow " + this.execId);
      this.flow.setStatus(Status.KILLING);
      // If the flow is paused, then we'll also unpause
      this.flowPaused = false;
      this.flowKilled = true;

      if (this.watcher != null) {
        this.logger.info("Watcher is attached. Stopping watcher.");
        this.watcher.stopWatcher();
        this.logger
            .info("Watcher cancelled status is " + this.watcher.isWatchCancelled());
      }

      this.logger.info("Killing " + this.activeJobRunners.size() + " jobs.");
      for (final JobRunner runner : this.activeJobRunners) {
        runner.kill();
      }
      updateFlow();
    }
    interrupt();
  }

  public void retryFailures(final String user) {
    synchronized (this.mainSyncObj) {
      this.logger.info("Retrying failures invoked by " + user);
      this.retryFailedJobs = true;
      interrupt();
    }
  }

  private void resetFailedState(final ExecutableFlowBase flow,
      final List<ExecutableNode> nodesToRetry) {
    // bottom up
    final LinkedList<ExecutableNode> queue = new LinkedList<>();
    for (final String id : flow.getEndNodes()) {
      final ExecutableNode node = flow.getExecutableNode(id);
      queue.add(node);
    }

    long maxStartTime = -1;
    while (!queue.isEmpty()) {
      final ExecutableNode node = queue.poll();
      final Status oldStatus = node.getStatus();
      maxStartTime = Math.max(node.getStartTime(), maxStartTime);

      final long currentTime = System.currentTimeMillis();
      if (node.getStatus() == Status.SUCCEEDED) {
        // This is a candidate parent for restart
        nodesToRetry.add(node);
        continue;
      } else if (node.getStatus() == Status.RUNNING) {
        continue;
      } else if (node.getStatus() == Status.KILLING) {
        continue;
      } else if (node.getStatus() == Status.SKIPPED) {
        node.setStatus(Status.DISABLED);
        node.setEndTime(-1);
        node.setStartTime(-1);
        node.setUpdateTime(currentTime);
      } else if (node instanceof ExecutableFlowBase) {
        final ExecutableFlowBase base = (ExecutableFlowBase) node;
        switch (base.getStatus()) {
          case CANCELLED:
            node.setStatus(Status.READY);
            node.setEndTime(-1);
            node.setStartTime(-1);
            node.setUpdateTime(currentTime);
            // Break out of the switch. We'll reset the flow just like a normal
            // node
            break;
          case KILLED:
          case FAILED:
          case FAILED_FINISHING:
            resetFailedState(base, nodesToRetry);
            continue;
          default:
            // Continue the while loop. If the job is in a finished state that's
            // not
            // a failure, we don't want to reset the job.
            continue;
        }
      } else if (node.getStatus() == Status.CANCELLED) {
        // Not a flow, but killed
        node.setStatus(Status.READY);
        node.setStartTime(-1);
        node.setEndTime(-1);
        node.setUpdateTime(currentTime);
      } else if (node.getStatus() == Status.FAILED
          || node.getStatus() == Status.KILLED) {
        node.resetForRetry();
        nodesToRetry.add(node);
      }

      if (!(node instanceof ExecutableFlowBase)
          && node.getStatus() != oldStatus) {
        this.logger.info("Resetting job '" + node.getNestedId() + "' from "
            + oldStatus + " to " + node.getStatus());
      }

      for (final String inId : node.getInNodes()) {
        final ExecutableNode nodeUp = flow.getExecutableNode(inId);
        queue.add(nodeUp);
      }
    }

    // At this point, the following code will reset the flow
    final Status oldFlowState = flow.getStatus();
    if (maxStartTime == -1) {
      // Nothing has run inside the flow, so we assume the flow hasn't even
      // started running yet.
      flow.setStatus(Status.READY);
    } else {
      flow.setStatus(Status.RUNNING);

      // Add any READY start nodes. Usually it means the flow started, but the
      // start node has not.
      for (final String id : flow.getStartNodes()) {
        final ExecutableNode node = flow.getExecutableNode(id);
        if (node.getStatus() == Status.READY
            || node.getStatus() == Status.DISABLED) {
          nodesToRetry.add(node);
        }
      }
    }
    flow.setUpdateTime(System.currentTimeMillis());
    flow.setEndTime(-1);
    this.logger.info("Resetting flow '" + flow.getNestedId() + "' from "
        + oldFlowState + " to " + flow.getStatus());
  }

  private void interrupt() {
    this.flowRunnerThread.interrupt();
  }

  public boolean isKilled() {
    return this.flowKilled;
  }

  public ExecutableFlow getExecutableFlow() {
    return this.flow;
  }

  public File getFlowLogFile() {
    return this.logFile;
  }

  public File getJobLogFile(final String jobId, final int attempt) {
    final ExecutableNode node = this.flow.getExecutableNodePath(jobId);
    final File path = new File(this.execDir, node.getJobSource());

    final String logFileName = JobRunner.createLogFileName(node, attempt);
    final File logFile = new File(path.getParentFile(), logFileName);

    if (!logFile.exists()) {
      return null;
    }

    return logFile;
  }

  public File getJobAttachmentFile(final String jobId, final int attempt) {
    final ExecutableNode node = this.flow.getExecutableNodePath(jobId);
    final File path = new File(this.execDir, node.getJobSource());

    final String attachmentFileName =
        JobRunner.createAttachmentFileName(node, attempt);
    final File attachmentFile = new File(path.getParentFile(), attachmentFileName);
    if (!attachmentFile.exists()) {
      return null;
    }
    return attachmentFile;
  }

  public File getJobMetaDataFile(final String jobId, final int attempt) {
    final ExecutableNode node = this.flow.getExecutableNodePath(jobId);
    final File path = new File(this.execDir, node.getJobSource());

    final String metaDataFileName = JobRunner.createMetaDataFileName(node, attempt);
    final File metaDataFile = new File(path.getParentFile(), metaDataFileName);

    if (!metaDataFile.exists()) {
      return null;
    }

    return metaDataFile;
  }

  public boolean isRunnerThreadAlive() {
    if (this.flowRunnerThread != null) {
      return this.flowRunnerThread.isAlive();
    }
    return false;
  }

  public boolean isThreadPoolShutdown() {
    return this.executorService.isShutdown();
  }

  public int getNumRunningJobs() {
    return this.activeJobRunners.size();
  }

  public int getExecutionId() {
    return this.execId;
  }

  public Set<JobRunner> getActiveJobRunners() {
    return ImmutableSet.copyOf(this.activeJobRunners);
  }

  // Class helps report the flow start and stop events.
  private class FlowRunnerEventListener implements EventListener {

    public FlowRunnerEventListener() {
    }

    private synchronized Map<String, String> getFlowMetadata(final FlowRunner flowRunner) {
      final ExecutableFlow flow = flowRunner.getExecutableFlow();
      final Props props = ServiceProvider.SERVICE_PROVIDER.getInstance(Props.class);
      final Map<String, String> metaData = new HashMap<>();
      metaData.put("flowName", flow.getId());
      metaData.put("azkabanHost", props.getString(AZKABAN_SERVER_HOST_NAME, "unknown"));
      metaData.put("projectName", flow.getProjectName());
      metaData.put("submitUser", flow.getSubmitUser());
      metaData.put("executionId", String.valueOf(flow.getExecutionId()));
      metaData.put("startTime", String.valueOf(flow.getStartTime()));
      metaData.put("submitTime", String.valueOf(flow.getSubmitTime()));
      return metaData;
    }

    @Override
    public synchronized void handleEvent(final Event event) {
      if (event.getType() == EventType.FLOW_STARTED) {
        final FlowRunner flowRunner = (FlowRunner) event.getRunner();
        final ExecutableFlow flow = flowRunner.getExecutableFlow();
        FlowRunner.this.logger.info("Flow started: " + flow.getId());
        FlowRunner.this.azkabanEventReporter.report(event.getType(), getFlowMetadata(flowRunner));
      } else if (event.getType() == EventType.FLOW_FINISHED) {
        final FlowRunner flowRunner = (FlowRunner) event.getRunner();
        final ExecutableFlow flow = flowRunner.getExecutableFlow();
        FlowRunner.this.logger.info("Flow ended: " + flow.getId());
        final Map<String, String> flowMetadata = getFlowMetadata(flowRunner);
        flowMetadata.put("endTime", String.valueOf(flow.getEndTime()));
        flowMetadata.put("flowStatus", flow.getStatus().name());
        FlowRunner.this.azkabanEventReporter.report(event.getType(), flowMetadata);
      }
    }
  }

  private class JobRunnerEventListener implements EventListener {

    public JobRunnerEventListener() {
    }

    private synchronized Map<String, String> getJobMetadata(final JobRunner jobRunner) {
      final ExecutableNode node = jobRunner.getNode();
      final Props props = ServiceProvider.SERVICE_PROVIDER.getInstance(Props.class);
      final Map<String, String> metaData = new HashMap<>();
      metaData.put("jobId", node.getId());
      metaData.put("executionID", String.valueOf(node.getExecutableFlow().getExecutionId()));
      metaData.put("flowName", node.getExecutableFlow().getId());
      metaData.put("startTime", String.valueOf(node.getStartTime()));
      metaData.put("jobType", String.valueOf(node.getType()));
      metaData.put("azkabanHost", props.getString(AZKABAN_SERVER_HOST_NAME, "unknown"));
      metaData.put("jobProxyUser",
          jobRunner.getProps().getString(JobProperties.USER_TO_PROXY, null));
      return metaData;
    }

    @Override
    public synchronized void handleEvent(final Event event) {
      if (event.getType() == EventType.JOB_STATUS_CHANGED) {
        updateFlow();
      } else if (event.getType() == EventType.JOB_FINISHED) {
        final EventData eventData = event.getData();
        final JobRunner jobRunner = (JobRunner) event.getRunner();
        final ExecutableNode node = jobRunner.getNode();
        if (FlowRunner.this.azkabanEventReporter != null) {
          final Map<String, String> jobMetadata = getJobMetadata(jobRunner);
          jobMetadata.put("jobStatus", node.getStatus().name());
          jobMetadata.put("endTime", String.valueOf(node.getEndTime()));
          FlowRunner.this.azkabanEventReporter.report(event.getType(), jobMetadata);
        }
        final long seconds = (node.getEndTime() - node.getStartTime()) / 1000;
        synchronized (FlowRunner.this.mainSyncObj) {
          FlowRunner.this.logger.info("Job " + eventData.getNestedId() + " finished with status "
              + eventData.getStatus() + " in " + seconds + " seconds");

          // Cancellation is handled in the main thread, but if the flow is
          // paused, the main thread is paused too.
          // This unpauses the flow for cancellation.
          if (FlowRunner.this.flowPaused && eventData.getStatus() == Status.FAILED
              && FlowRunner.this.failureAction == FailureAction.CANCEL_ALL) {
            FlowRunner.this.flowPaused = false;
          }

          FlowRunner.this.finishedNodes.add(node);
          FlowRunner.this.activeJobRunners.remove(jobRunner);
          node.getParentFlow().setUpdateTime(System.currentTimeMillis());
          interrupt();
          fireEventListeners(event);
        }
      } else if (event.getType() == EventType.JOB_STARTED) {
        final EventData eventData = event.getData();
        FlowRunner.this.logger.info("Job Started: " + eventData.getNestedId());
        if (FlowRunner.this.azkabanEventReporter != null) {
          final JobRunner jobRunner = (JobRunner) event.getRunner();
          FlowRunner.this.azkabanEventReporter.report(event.getType(), getJobMetadata(jobRunner));
        }
        // add job level checker
        final TriggerManager triggerManager = ServiceProvider.SERVICE_PROVIDER
            .getInstance(TriggerManager.class);
        triggerManager
            .addTrigger(FlowRunner.this.flow.getExecutionId(), SlaOption.getJobLevelSLAOptions(
                FlowRunner.this.flow));
      }
    }
  }
}

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

import azkaban.event.Event;
import azkaban.event.Event.Type;
import azkaban.event.EventHandler;
import azkaban.event.EventListener;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.JobCallbackManager;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.execapp.metric.JobDurationMetric;
import azkaban.execapp.metric.NumFailedJobMetric;
import azkaban.execapp.metric.NumRunningJobMetric;
import azkaban.execapp.metric.TotalNumFlowJobsMetric;
import azkaban.executor.*;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.flow.FlowProps;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobtype.JobTypeManager;
import azkaban.metric.MetricReportManager;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.SwapQueue;
import org.apache.log4j.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * Class that handles the running of a ExecutableFlow DAG
 */
public class FlowRunner extends EventHandler implements Runnable {
    private static final Layout DEFAULT_LAYOUT = new PatternLayout(
            "%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");
    // We check update every 5 minutes, just in case things get stuck. But for the
    // most part, we'll be idling.
    private static final long CHECK_WAIT_MS = 5 * 60 * 1000;

    private Logger logger;

    private Layout loggerLayout = DEFAULT_LAYOUT;
    private Appender flowAppender;
    private File logFile;

    private ExecutorService executorService;
    private ExecutorLoader executorLoader;
    private ProjectLoader projectLoader;

    private int execId;
    private File execDir;
    private final ExecutableFlow flow;
    private Thread flowRunnerThread;
    private int numJobThreads = 10;
    private ExecutionOptions.FailureAction failureAction;

    // Sync object for queuing
    private final Object mainSyncObj = new Object();

    // Properties map
    private Map<String, Props> sharedProps = new HashMap<String, Props>();
    private final JobTypeManager jobtypeManager;

    private JobRunnerEventListener listener = new JobRunnerEventListener();
    private Set<JobRunner> activeJobRunners = Collections
            .newSetFromMap(new ConcurrentHashMap<JobRunner, Boolean>());

    // Thread safe swap queue for finishedExecutions.
    private SwapQueue<ExecutableNode> finishedNodes;

    // Used for pipelining
    private Integer pipelineLevel = null;
    private Integer pipelineExecId = null;

    // Watches external flows for execution.
    private FlowWatcher watcher = null;

    private Set<String> proxyUsers = null;
    private boolean validateUserProxy;

    private String jobLogFileSize = "5MB";
    private int jobLogNumFiles = 4;

    private boolean flowPaused = false;
    private boolean flowFailed = false;
    private boolean flowFinished = false;
    private boolean flowKilled = false;

    // The following is state that will trigger a retry of all failed jobs
    private boolean retryFailedJobs = false;

    /**
     * Constructor. This will create its own ExecutorService for thread pools
     *
     * @param flow
     * @param executorLoader
     * @param projectLoader
     * @param jobtypeManager
     * @throws ExecutorManagerException
     */
    public FlowRunner(ExecutableFlow flow, ExecutorLoader executorLoader,
                      ProjectLoader projectLoader, JobTypeManager jobtypeManager)
            throws ExecutorManagerException {
        this(flow, executorLoader, projectLoader, jobtypeManager, null);
    }

    /**
     * Constructor. If executorService is null, then it will create it's own for
     * thread pools.
     *
     * @param flow
     * @param executorLoader
     * @param projectLoader
     * @param jobtypeManager
     * @param executorService
     * @throws ExecutorManagerException
     */
    public FlowRunner(ExecutableFlow flow, ExecutorLoader executorLoader,
                      ProjectLoader projectLoader, JobTypeManager jobtypeManager,
                      ExecutorService executorService) throws ExecutorManagerException {
        this.execId = flow.getExecutionId();
        this.flow = flow;
        this.executorLoader = executorLoader;
        this.projectLoader = projectLoader;
        this.execDir = new File(flow.getExecutionPath());
        this.jobtypeManager = jobtypeManager;

        ExecutionOptions options = flow.getExecutionOptions();
        this.pipelineLevel = options.getPipelineLevel();
        this.pipelineExecId = options.getPipelineExecutionId();
        this.failureAction = options.getFailureAction();
        this.proxyUsers = flow.getProxyUsers();
        this.executorService = executorService;
        this.finishedNodes = new SwapQueue<ExecutableNode>();

        this.addListener(AzkabanExecutorServer.getApp().getClusterManager());

    }

    public FlowRunner setFlowWatcher(FlowWatcher watcher) {
        this.watcher = watcher;
        return this;
    }

    public FlowRunner setNumJobThreads(int jobs) {
        numJobThreads = jobs;
        return this;
    }

    public FlowRunner setJobLogSettings(String jobLogFileSize, int jobLogNumFiles) {
        this.jobLogFileSize = jobLogFileSize;
        this.jobLogNumFiles = jobLogNumFiles;

        return this;
    }

    public FlowRunner setValidateProxyUser(boolean validateUserProxy) {
        this.validateUserProxy = validateUserProxy;
        return this;
    }

    public File getExecutionDir() {
        return execDir;
    }



    public void run() {
        try {
            if (this.executorService == null) {
                this.executorService = Executors.newFixedThreadPool(numJobThreads);
            }
            setupFlowExecution();
            flow.setStartTime(System.currentTimeMillis());

            updateFlowReference();

            logger.info("Updating initial flow directory.");
            updateFlow();
            logger.info("Fetching job and shared properties.");
            loadAllProperties();


            this.fireEventListeners(Event.create(this, Type.FLOW_STARTED));
            runFlow();

        } catch (Throwable t) {
            if (logger != null) {
                logger.error("An error has occurred during the running of the flow. Quiting.", t);
            }
            flow.setStatus(Status.FAILED);
        } finally {
            if (watcher != null) {
                logger.info("Watcher is attached. Stopping watcher.");
                watcher.stopWatcher();
                logger
                        .info("Watcher cancelled status is " + watcher.isWatchCancelled());
            }

            flow.setEndTime(System.currentTimeMillis());
            logger.info("Setting end time for flow " + execId + " to "
                    + System.currentTimeMillis());


            updateFlow();
            this.fireEventListeners(Event.create(this, Type.FLOW_FINISHED));
            closeLogger();
        }
    }


    @SuppressWarnings("unchecked")
    private void setupFlowExecution() {
        int projectId = flow.getProjectId();
        int version = flow.getVersion();
        String flowId = flow.getFlowId();

        // Add a bunch of common azkaban properties
        Props commonFlowProps = PropsUtils.addCommonFlowProperties(null, flow);

        if (flow.getJobSource() != null) {
            String source = flow.getJobSource();
            Props flowProps = sharedProps.get(source);
            flowProps.setParent(commonFlowProps);
            commonFlowProps = flowProps;
        }

        // If there are flow overrides, we apply them now.
        Map<String, String> flowParam =
                flow.getExecutionOptions().getFlowParameters();
        if (flowParam != null && !flowParam.isEmpty()) {
            commonFlowProps = new Props(commonFlowProps, flowParam);
        }
        flow.setInputProps(commonFlowProps);

        // Create execution dir
        createLogger(flowId);

        if (this.watcher != null) {
            this.watcher.setLogger(logger);
        }

        logger.info("Assigned executor : " + AzkabanExecutorServer.getApp().getExecutorHostPort());
        logger.info("Running execid:" + execId + " flow:" + flowId + " project:"
                + projectId + " version:" + version);
        if (pipelineExecId != null) {
            logger.info("Running simulateously with " + pipelineExecId
                    + ". Pipelining level " + pipelineLevel);
        }

        // The current thread is used for interrupting blocks
        flowRunnerThread = Thread.currentThread();
        flowRunnerThread.setName("FlowRunner-exec-" + flow.getExecutionId());
    }

    private void updateFlowReference() throws ExecutorManagerException {
        logger.info("Update active reference");
        if (!executorLoader.updateExecutableReference(execId,
                System.currentTimeMillis())) {
            throw new ExecutorManagerException(
                    "The executor reference doesn't exist. May have been killed prematurely.");
        }
    }

    private void updateFlow() {
        updateFlow(System.currentTimeMillis());
    }

    private synchronized void updateFlow(long time) {
        try {
            flow.setUpdateTime(time);
            executorLoader.updateExecutableFlow(flow);
        } catch (ExecutorManagerException e) {
            logger.error("Error updating flow.", e);
        }
    }

    private void createLogger(String flowId) {
        // Create logger
        String loggerName = execId + "." + flowId;
        logger = Logger.getLogger(loggerName);

        // Create file appender
        String logName = "_flow." + loggerName + ".log";
        logFile = new File(execDir, logName);
        String absolutePath = logFile.getAbsolutePath();

        flowAppender = null;
        try {
            flowAppender = new FileAppender(loggerLayout, absolutePath, false);
            logger.addAppender(flowAppender);
        } catch (IOException e) {
            logger.error("Could not open log file in " + execDir, e);
        }
    }

    private void closeLogger() {
        if (logger != null) {
            logger.removeAppender(flowAppender);
            flowAppender.close();

            try {
                executorLoader.uploadLogFile(execId, "", 0, logFile);
            } catch (ExecutorManagerException e) {
                e.printStackTrace();
            }
        }
    }

    private void loadAllProperties() throws IOException {
        // First load all the properties
        for (FlowProps fprops : flow.getFlowProps()) {
            String source = fprops.getSource();
            File propsPath = new File(execDir, source);
            Props props = new Props(null, propsPath);
            sharedProps.put(source, props);
        }

        // Resolve parents
        for (FlowProps fprops : flow.getFlowProps()) {
            if (fprops.getInheritedSource() != null) {
                String source = fprops.getSource();
                String inherit = fprops.getInheritedSource();

                Props props = sharedProps.get(source);
                Props inherits = sharedProps.get(inherit);

                props.setParent(inherits);
            }
        }
    }

    /**
     * Main method that executes the jobs.
     *
     * @throws Exception
     */
    private void runFlow() throws Exception {
        logger.info("Starting flows");
        runReadyJob(this.flow);
        updateFlow();

        while (!flowFinished) {
            synchronized (mainSyncObj) {
                if (flowPaused) {
                    try {
                        mainSyncObj.wait(CHECK_WAIT_MS);
                    } catch (InterruptedException e) {
                    }

                    continue;
                } else {
                    if (retryFailedJobs) {
                        retryAllFailures();
                    } else if (!progressGraph()) {
                        try {
                            mainSyncObj.wait(CHECK_WAIT_MS);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            }
        }

        logger.info("Finishing up flow. Awaiting Termination");
        executorService.shutdown();

        updateFlow();
        logger.info("Finished Flow");
    }

    private void retryAllFailures() throws IOException {
        logger.info("Restarting all failed jobs");

        this.retryFailedJobs = false;
        this.flowKilled = false;
        this.flowFailed = false;
        this.flow.setStatus(Status.RUNNING);

        ArrayList<ExecutableNode> retryJobs = new ArrayList<ExecutableNode>();
        resetFailedState(this.flow, retryJobs);

        for (ExecutableNode node : retryJobs) {
            if (node.getStatus() == Status.READY
                    || node.getStatus() == Status.DISABLED) {
                runReadyJob(node);
            } else if (node.getStatus() == Status.SUCCEEDED) {
                for (String outNodeId : node.getOutNodes()) {
                    ExecutableFlowBase base = node.getParentFlow();
                    runReadyJob(base.getExecutableNode(outNodeId));
                }
            }

            runReadyJob(node);
        }

        updateFlow();
    }

    private boolean progressGraph() throws IOException {
        finishedNodes.swap();

        // The following nodes are finished, so we'll collect a list of outnodes
        // that are candidates for running next.
        HashSet<ExecutableNode> nodesToCheck = new HashSet<ExecutableNode>();
        for (ExecutableNode node : finishedNodes) {
            Set<String> outNodeIds = node.getOutNodes();
            ExecutableFlowBase parentFlow = node.getParentFlow();

            // If a job is seen as failed, then we set the parent flow to
            // FAILED_FINISHING
            if (node.getStatus() == Status.FAILED) {
                // The job cannot be retried or has run out of retry attempts. We will
                // fail the job and its flow now.
                if (!retryJobIfPossible(node)) {
                    propagateStatus(node.getParentFlow(), Status.FAILED_FINISHING);
                    if (failureAction == FailureAction.CANCEL_ALL) {
                        this.kill();
                    }
                    this.flowFailed = true;
                } else {
                    nodesToCheck.add(node);
                    continue;
                }
            }

            if (outNodeIds.isEmpty()) {
                // There's no outnodes means it's the end of a flow, so we finalize
                // and fire an event.
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
            for (String nodeId : outNodeIds) {
                ExecutableNode outNode = parentFlow.getExecutableNode(nodeId);
                nodesToCheck.add(outNode);
            }
        }

        // Runs candidate jobs. The code will check to see if they are ready to run
        // before
        // Instant kill or skip if necessary.
        boolean jobsRun = false;
        for (ExecutableNode node : nodesToCheck) {
            if (Status.isStatusFinished(node.getStatus())
                    || Status.isStatusRunning(node.getStatus())) {
                // Really shouldn't get in here.
                continue;
            }

            jobsRun |= runReadyJob(node);
        }

        if (jobsRun || finishedNodes.getSize() > 0) {
            updateFlow();
            return true;
        }

        return false;
    }

    private boolean runReadyJob(ExecutableNode node) throws IOException {
        if (Status.isStatusFinished(node.getStatus())
                || Status.isStatusRunning(node.getStatus())) {
            return false;
        }

        Status nextNodeStatus = getImpliedStatus(node);
        if (nextNodeStatus == null) {
            return false;
        }

        if (nextNodeStatus == Status.CANCELLED) {
            logger.info("Cancelling '" + node.getNestedId()
                    + "' due to prior errors.");
            node.cancelNode(System.currentTimeMillis());
            finishExecutableNode(node);
        } else if (nextNodeStatus == Status.SKIPPED) {
            logger.info("Skipping disabled job '" + node.getId() + "'.");
            node.skipNode(System.currentTimeMillis());
            finishExecutableNode(node);
        } else if (nextNodeStatus == Status.READY) {
            if (node instanceof ExecutableFlowBase) {
                ExecutableFlowBase flow = ((ExecutableFlowBase) node);
                logger.info("Running flow '" + flow.getNestedId() + "'.");
                flow.setStatus(Status.RUNNING);
                flow.setStartTime(System.currentTimeMillis());
                prepareJobProperties(flow);

                for (String startNodeId : ((ExecutableFlowBase) node).getStartNodes()) {
                    ExecutableNode startNode = flow.getExecutableNode(startNodeId);
                    runReadyJob(startNode);
                }
            } else {
                runExecutableNode(node);
            }
        }
        return true;
    }

    private boolean retryJobIfPossible(ExecutableNode node) {
        if (node instanceof ExecutableFlowBase) {
            return false;
        }

        if (node.getRetries() > node.getAttempt()) {
            logger.info("Job '" + node.getId() + "' will be retried. Attempt "
                    + node.getAttempt() + " of " + node.getRetries());
            node.setDelayedExecution(node.getRetryBackoff());
            node.resetForRetry();
            return true;
        } else {
            if (node.getRetries() > 0) {
                logger.info("Job '" + node.getId() + "' has run out of retry attempts");
                // Setting delayed execution to 0 in case this is manually re-tried.
                node.setDelayedExecution(0);
            }

            return false;
        }
    }

    private void propagateStatus(ExecutableFlowBase base, Status status) {
        if (!Status.isStatusFinished(base.getStatus())) {
            logger.info("Setting " + base.getNestedId() + " to " + status);
            base.setStatus(status);
            if (base.getParentFlow() != null) {
                propagateStatus(base.getParentFlow(), status);
            }
        }
    }

    private void finishExecutableNode(ExecutableNode node) {
        finishedNodes.add(node);
        fireEventListeners(Event.create(this, Type.JOB_FINISHED, node));
    }

    private void finalizeFlow(ExecutableFlowBase flow) {
        String id = flow == this.flow ? "" : flow.getNestedId();

        // If it's not the starting flow, we'll create set of output props
        // for the finished flow.
        boolean succeeded = true;
        Props previousOutput = null;

        for (String end : flow.getEndNodes()) {
            ExecutableNode node = flow.getExecutableNode(end);

            if (node.getStatus() == Status.KILLED
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
        long durationSec = (flow.getEndTime() - flow.getStartTime()) / 1000;
        switch (flow.getStatus()) {
            case FAILED_FINISHING:
                logger.info("Setting flow '" + id + "' status to FAILED in "
                        + durationSec + " seconds");
                flow.setStatus(Status.FAILED);
                break;
            case FAILED:
            case KILLED:
            case CANCELLED:
            case FAILED_SUCCEEDED:
                logger.info("Flow '" + id + "' is set to " + flow.getStatus().toString()
                        + " in " + durationSec + " seconds");
                break;
            default:
                flow.setStatus(Status.SUCCEEDED);
                logger.info("Flow '" + id + "' is set to " + flow.getStatus().toString()
                        + " in " + durationSec + " seconds");
        }

        // If the finalized flow is actually the top level flow, than we finish
        // the main loop.
        if (flow instanceof ExecutableFlow) {
            flowFinished = true;
        }
    }

    private void prepareJobProperties(ExecutableNode node) throws IOException {
        if (node instanceof ExecutableFlow) {
            return;
        }

        Props props = null;
        // 1. Shared properties (i.e. *.properties) for the jobs only. This takes
        // the
        // least precedence
        if (!(node instanceof ExecutableFlowBase)) {
            String sharedProps = node.getPropsSource();
            if (sharedProps != null) {
                props = this.sharedProps.get(sharedProps);
            }
        }

        // The following is the hiearchical ordering of dependency resolution
        // 2. Parent Flow Properties
        ExecutableFlowBase parentFlow = node.getParentFlow();
        if (parentFlow != null) {
            Props flowProps = Props.clone(parentFlow.getInputProps());
            flowProps.setEarliestAncestor(props);
            props = flowProps;
        }

        // 3. Output Properties. The call creates a clone, so we can overwrite it.
        Props outputProps = collectOutputProps(node);
        if (outputProps != null) {
            outputProps.setEarliestAncestor(props);
            props = outputProps;
        }

        // 4. The job source.
        Props jobSource = loadJobProps(node);
        if (jobSource != null) {
            jobSource.setParent(props);
            props = jobSource;
        }

        node.setInputProps(props);
    }

    /**
     * @param props This method is to put in any job properties customization before feeding
     *              to the job.
     */
    private void customizeJobProperties(Props props) {
        boolean memoryCheck = flow.getExecutionOptions().getMemoryCheck();
        props.put(ProcessJob.AZKABAN_MEMORY_CHECK, Boolean.toString(memoryCheck));
    }

    private Props loadJobProps(ExecutableNode node) throws IOException {
        Props props = null;
        String source = node.getJobSource();
        if (source == null) {
            return null;
        }

        // load the override props if any
        try {
            props =
                    projectLoader.fetchProjectProperty(flow.getProjectId(),
                            flow.getVersion(), node.getId() + ".jor");
        } catch (ProjectManagerException e) {
            e.printStackTrace();
            logger.error("Error loading job override property for job "
                    + node.getId());
        }

        File path = new File(execDir, source);
        if (props == null) {
            // if no override prop, load the original one on disk
            try {
                props = new Props(null, path);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("Error loading job file " + source + " for job "
                        + node.getId());
            }
        }
        // setting this fake source as this will be used to determine the location
        // of log files.
        if (path.getPath() != null) {
            props.setSource(path.getPath());
        }

        customizeJobProperties(props);

        return props;
    }

    private void runExecutableNode(ExecutableNode node) throws IOException {
        // Collect output props from the job's dependencies.
        prepareJobProperties(node);

        node.setStatus(Status.QUEUED);
        JobRunner runner = createJobRunner(node);
        logger.info("Submitting job '" + node.getNestedId() + "' to run.");
        try {
            executorService.submit(runner);
            activeJobRunners.add(runner);
        } catch (RejectedExecutionException e) {
            logger.error(e);
        }
        ;
    }

    /**
     * Determines what the state of the next node should be. Returns null if the
     * node should not be run.
     *
     * @param node
     * @return
     */
    public Status getImpliedStatus(ExecutableNode node) {
        // If it's running or finished with 'SUCCEEDED', than don't even
        // bother starting this job.
        if (Status.isStatusRunning(node.getStatus())
                || node.getStatus() == Status.SUCCEEDED) {
            return null;
        }

        // Go through the node's dependencies. If all of the previous job's
        // statuses is finished and not FAILED or KILLED, than we can safely
        // run this job.
        ExecutableFlowBase flow = node.getParentFlow();
        boolean shouldKill = false;
        for (String dependency : node.getInNodes()) {
            ExecutableNode dependencyNode = flow.getExecutableNode(dependency);
            Status depStatus = dependencyNode.getStatus();

            if (!Status.isStatusFinished(depStatus)) {
                return null;
            } else if (depStatus == Status.FAILED || depStatus == Status.CANCELLED
                    || depStatus == Status.KILLED) {
                // We propagate failures as KILLED states.
                shouldKill = true;
            }
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
        if (flowFailed
                && failureAction == ExecutionOptions.FailureAction.FINISH_CURRENTLY_RUNNING) {
            return Status.CANCELLED;
        } else if (shouldKill || isKilled()) {
            return Status.CANCELLED;
        }

        // All good to go, ready to run.
        return Status.READY;
    }

    private Props collectOutputProps(ExecutableNode node) {
        Props previousOutput = null;
        // Iterate the in nodes again and create the dependencies
        for (String dependency : node.getInNodes()) {
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

    private JobRunner createJobRunner(ExecutableNode node) {
        // Load job file.
        File path = new File(execDir, node.getJobSource());

        JobRunner jobRunner =
                new JobRunner(node, path.getParentFile(), executorLoader,
                        jobtypeManager);
        if (watcher != null) {
            jobRunner.setPipeline(watcher, pipelineLevel);
        }
        if (validateUserProxy) {
            jobRunner.setValidatedProxyUsers(proxyUsers);
        }

        jobRunner.setDelayStart(node.getDelayedExecution());
        jobRunner.setLogSettings(logger, jobLogFileSize, jobLogNumFiles);
        jobRunner.addListener(listener);

        if (JobCallbackManager.isInitialized()) {
            jobRunner.addListener(JobCallbackManager.getInstance());
        }

        configureJobLevelMetrics(jobRunner);

        return jobRunner;
    }

    /**
     * Configure Azkaban metrics tracking for a new jobRunner instance
     *
     * @param jobRunner
     */
    private void configureJobLevelMetrics(JobRunner jobRunner) {
        logger.info("Configuring Azkaban metrics tracking for jobrunner object");
        if (MetricReportManager.isAvailable()) {
            MetricReportManager metricManager = MetricReportManager.getInstance();

            // Adding NumRunningJobMetric listener
            jobRunner.addListener((NumRunningJobMetric) metricManager
                    .getMetricFromName(NumRunningJobMetric.NUM_RUNNING_JOB_METRIC_NAME));

            // Adding NumFailedJobMetric listener
            jobRunner.addListener((NumFailedJobMetric) metricManager
                    .getMetricFromName(NumFailedJobMetric.NUM_FAILED_JOB_METRIC_NAME));

            // Adding JobDurationMetric listener
            jobRunner.addListener((JobDurationMetric) metricManager
                    .getMetricFromName(JobDurationMetric.JOB_DURATION_METRIC_NAME));

            // Adding TotalNumFlowJobsMetric Listener
            jobRunner.addListener((TotalNumFlowJobsMetric) metricManager
                    .getMetricFromName(TotalNumFlowJobsMetric.TOTAL_NUM_FLOW_JOBS_METRIC_NAME));

        }

        jobRunner.addListener(JmxJobMBeanManager.getInstance());
    }

    public void pause(String user) {
        synchronized (mainSyncObj) {
            if (!flowFinished) {
                logger.info("Flow paused by " + user);
                flowPaused = true;
                flow.setStatus(Status.PAUSED);

                updateFlow();
            } else {
                logger.info("Cannot pause finished flow. Called by user " + user);
            }
        }

        interrupt();
    }

    public void resume(String user) {
        synchronized (mainSyncObj) {
            if (!flowPaused) {
                logger.info("Cannot resume flow that isn't paused");
            } else {
                logger.info("Flow resumed by " + user);
                flowPaused = false;
                if (flowFailed) {
                    flow.setStatus(Status.FAILED_FINISHING);
                } else if (flowKilled) {
                    flow.setStatus(Status.KILLED);
                } else {
                    flow.setStatus(Status.RUNNING);
                }

                updateFlow();
            }
        }

        interrupt();
    }

    public void kill(String user) {
        synchronized (mainSyncObj) {
            logger.info("Flow killed by " + user);
            flow.setStatus(Status.KILLED);
            kill();
            updateFlow();
        }
        interrupt();
    }

    private void kill() {
        synchronized (mainSyncObj) {
            logger.info("Kill has been called on flow " + execId);

            // If the flow is paused, then we'll also unpause
            flowPaused = false;
            flowKilled = true;

            if (watcher != null) {
                logger.info("Watcher is attached. Stopping watcher.");
                watcher.stopWatcher();
                logger
                        .info("Watcher cancelled status is " + watcher.isWatchCancelled());
            }

            logger.info("Killing " + activeJobRunners.size() + " jobs.");
            for (JobRunner runner : activeJobRunners) {
                runner.kill();
            }
        }
    }

    public void retryFailures(String user) {
        synchronized (mainSyncObj) {
            logger.info("Retrying failures invoked by " + user);
            retryFailedJobs = true;
            interrupt();
        }
    }

    private void resetFailedState(ExecutableFlowBase flow,
                                  List<ExecutableNode> nodesToRetry) {
        // bottom up
        LinkedList<ExecutableNode> queue = new LinkedList<ExecutableNode>();
        for (String id : flow.getEndNodes()) {
            ExecutableNode node = flow.getExecutableNode(id);
            queue.add(node);
        }

        long maxStartTime = -1;
        while (!queue.isEmpty()) {
            ExecutableNode node = queue.poll();
            Status oldStatus = node.getStatus();
            maxStartTime = Math.max(node.getStartTime(), maxStartTime);

            long currentTime = System.currentTimeMillis();
            if (node.getStatus() == Status.SUCCEEDED) {
                // This is a candidate parent for restart
                nodesToRetry.add(node);
                continue;
            } else if (node.getStatus() == Status.RUNNING) {
                continue;
            } else if (node.getStatus() == Status.SKIPPED) {
                node.setStatus(Status.DISABLED);
                node.setEndTime(-1);
                node.setStartTime(-1);
                node.setUpdateTime(currentTime);
            } else if (node instanceof ExecutableFlowBase) {
                ExecutableFlowBase base = (ExecutableFlowBase) node;
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
                logger.info("Resetting job '" + node.getNestedId() + "' from "
                        + oldStatus + " to " + node.getStatus());
            }

            for (String inId : node.getInNodes()) {
                ExecutableNode nodeUp = flow.getExecutableNode(inId);
                queue.add(nodeUp);
            }
        }

        // At this point, the following code will reset the flow
        Status oldFlowState = flow.getStatus();
        if (maxStartTime == -1) {
            // Nothing has run inside the flow, so we assume the flow hasn't even
            // started running yet.
            flow.setStatus(Status.READY);
        } else {
            flow.setStatus(Status.RUNNING);

            // Add any READY start nodes. Usually it means the flow started, but the
            // start node has not.
            for (String id : flow.getStartNodes()) {
                ExecutableNode node = flow.getExecutableNode(id);
                if (node.getStatus() == Status.READY
                        || node.getStatus() == Status.DISABLED) {
                    nodesToRetry.add(node);
                }
            }
        }
        flow.setUpdateTime(System.currentTimeMillis());
        flow.setEndTime(-1);
        logger.info("Resetting flow '" + flow.getNestedId() + "' from "
                + oldFlowState + " to " + flow.getStatus());
    }

    private void interrupt() {
        flowRunnerThread.interrupt();
    }

    private class JobRunnerEventListener implements EventListener {
        public JobRunnerEventListener() {
        }

        @Override
        public synchronized void handleEvent(Event event) {
            JobRunner runner = (JobRunner) event.getRunner();

            if (event.getType() == Type.JOB_STATUS_CHANGED) {
                updateFlow();
            } else if (event.getType() == Type.JOB_FINISHED) {
                ExecutableNode node = runner.getNode();
                long seconds = (node.getEndTime() - node.getStartTime()) / 1000;
                synchronized (mainSyncObj) {
                    logger.info("Job " + node.getNestedId() + " finished with status "
                            + node.getStatus() + " in " + seconds + " seconds");

                    // Cancellation is handled in the main thread, but if the flow is
                    // paused, the main thread is paused too.
                    // This unpauses the flow for cancellation.
                    if (flowPaused && node.getStatus() == Status.FAILED
                            && failureAction == FailureAction.CANCEL_ALL) {
                        flowPaused = false;
                    }

                    finishedNodes.add(node);
                    node.getParentFlow().setUpdateTime(System.currentTimeMillis());
                    interrupt();
                    fireEventListeners(event);
                }
            }
        }
    }

    public boolean isKilled() {
        return flowKilled;
    }

    public ExecutableFlow getExecutableFlow() {
        return flow;
    }

    public File getFlowLogFile() {
        return logFile;
    }

    public File getJobLogFile(String jobId, int attempt) {
        ExecutableNode node = flow.getExecutableNodePath(jobId);
        File path = new File(execDir, node.getJobSource());

        String logFileName = JobRunner.createLogFileName(node, attempt);
        File logFile = new File(path.getParentFile(), logFileName);

        if (!logFile.exists()) {
            return null;
        }

        return logFile;
    }

    public File getJobAttachmentFile(String jobId, int attempt) {
        ExecutableNode node = flow.getExecutableNodePath(jobId);
        File path = new File(execDir, node.getJobSource());

        String attachmentFileName =
                JobRunner.createAttachmentFileName(node, attempt);
        File attachmentFile = new File(path.getParentFile(), attachmentFileName);
        if (!attachmentFile.exists()) {
            return null;
        }
        return attachmentFile;
    }

    public File getJobMetaDataFile(String jobId, int attempt) {
        ExecutableNode node = flow.getExecutableNodePath(jobId);
        File path = new File(execDir, node.getJobSource());

        String metaDataFileName = JobRunner.createMetaDataFileName(node, attempt);
        File metaDataFile = new File(path.getParentFile(), metaDataFileName);

        if (!metaDataFile.exists()) {
            return null;
        }

        return metaDataFile;
    }

    public boolean isRunnerThreadAlive() {
        if (flowRunnerThread != null) {
            return flowRunnerThread.isAlive();
        }
        return false;
    }

    public boolean isThreadPoolShutdown() {
        return executorService.isShutdown();
    }

    public int getNumRunningJobs() {
        return activeJobRunners.size();
    }

    public int getExecutionId() {
        return execId;
    }
}

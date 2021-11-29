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
package azkaban.execapp;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.DispatchMethod;
import azkaban.event.Event;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.InteractiveTestJob;
import azkaban.executor.MockExecutorLoader;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.jobtype.JobTypeManager;
import azkaban.jobtype.JobTypePluginSet;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.project.FlowLoader;
import azkaban.project.FlowLoaderFactory;
import azkaban.project.Project;
import azkaban.project.ProjectFileHandler;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import com.codahale.metrics.MetricRegistry;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowRunnerTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FlowRunnerTestUtil.class);

  private static int id = 101;
  private final Map<String, Flow> flowMap;
  private final Project project;
  private final File workingDir;
  private final JobTypeManager jobtypeManager;
  private final File projectDir;
  private final ProjectLoader projectLoader;
  private ExecutorLoader executorLoader;
  private final ProjectFileHandler handler;

  public FlowRunnerTestUtil(final String flowName, final TemporaryFolder temporaryFolder)
      throws Exception {

    this.projectDir = ExecutionsTestUtil.getFlowDir(flowName);
    this.workingDir = temporaryFolder.newFolder();
    this.project = new Project(1, "testProject");

    this.flowMap = FlowRunnerTestUtil
        .prepareProject(this.project, this.projectDir, this.workingDir);

    this.executorLoader = mock(ExecutorLoader.class);
    when(this.executorLoader.updateExecutableReference(anyInt(), anyLong())).thenReturn(true);

    this.projectLoader = mock(ProjectLoader.class);
    this.handler = new ProjectFileHandler(1, 1, 1, "testUser", "zip", "test.zip",
        1, null, null, null, "111.111.111.111");
    when(this.projectLoader.fetchProjectMetaData(anyInt(), anyInt())).thenReturn(this.handler);

    Utils.initServiceProvider();
    JmxJobMBeanManager.getInstance().initialize(new Props());

    InteractiveTestJob.clearTestJobs();

    this.jobtypeManager = new JobTypeManager(null, null, this.getClass().getClassLoader());
    final JobTypePluginSet pluginSet = this.jobtypeManager.getJobTypePluginSet();
    pluginSet.addPluginClassName("test", InteractiveTestJob.class.getName());
  }

  /**
   * Initialize the project with the flow definitions stored in the given source directory. Also
   * copy the source directory to the working directory.
   *
   * @param project    project to initialize
   * @param sourceDir  the source dir
   * @param workingDir the working dir
   * @return the flow name to flow map
   * @throws ProjectManagerException the project manager exception
   * @throws IOException             the io exception
   */
  public static Map<String, Flow> prepareProject(final Project project, final File sourceDir,
      final File workingDir)
      throws ProjectManagerException, IOException {
    final FlowLoaderFactory loaderFactory = new FlowLoaderFactory(new Props(null));
    final FlowLoader loader = loaderFactory.createFlowLoader(sourceDir);

    LOG.info("Loading project flows from " + sourceDir);
    loader.loadProjectFlow(project, sourceDir);
    if (!loader.getErrors().isEmpty()) {
      for (final String error : loader.getErrors()) {
        System.out.println(error);
      }
      throw new RuntimeException(String.format(
          "Errors found in loading flows into a project ( %s ). From the directory: ( %s ).",
          project.getName(), sourceDir));
    }

    final Map<String, Flow> flowMap = loader.getFlowMap();
    LOG.info("Loaded flows: " + flowMap.keySet());
    project.setFlows(flowMap);
    FileUtils.copyDirectory(sourceDir, workingDir);
    return flowMap;
  }

  public static void waitEventFired(final EventCollectorListener eventCollector,
      final String nestedId, final Status status)
      throws InterruptedException {
    for (int i = 0; i < 1000; i++) {
      for (final Event event : eventCollector.getEventList()) {
        if (event.getData().getStatus() == status && event.getData().getNestedId()
            .equals(nestedId)) {
          return;
        }
      }
      synchronized (EventCollectorListener.handleEvent) {
        EventCollectorListener.handleEvent.wait(10L);
      }
    }
    fail("Event wasn't fired with [" + nestedId + "], " + status);
  }

  public static ExecutableFlow prepareExecDir(final File workingDir, final File execDir,
      final String flowName, final int execId) throws IOException {
    FileUtils.copyDirectory(execDir, workingDir);
    final File jsonFlowFile = new File(workingDir, flowName + ".flow");
    final Object flowObj = JSONUtils.parseJSONFromFile(jsonFlowFile);
    final Project project = new Project(1, "test");
    final Flow flow = Flow.flowFromObject(flowObj);
    final ExecutableFlow execFlow = new ExecutableFlow(project, flow);
    execFlow.setExecutionId(execId);
    execFlow.setExecutionPath(workingDir.getPath());
    return execFlow;
  }

  public static Thread startThread(final FlowRunner runner) {
    final Thread thread = new Thread(runner);
    thread.start();
    return thread;
  }

  public FlowRunner createFromFlowFile(final String flowName) throws Exception {
    return createFromFlowFile(new EventCollectorListener(), flowName);
  }

  public FlowRunner createFromFlowFile(final EventCollectorListener eventCollector,
      final String flowName) throws Exception {
    return createFromFlowFile(flowName, eventCollector, new ExecutionOptions());
  }

  public FlowRunner createFromFlowFile(final String flowName,
      final EventCollectorListener eventCollector, final ExecutionOptions options)
      throws Exception {
    return createFromFlowFile(flowName, eventCollector, options, null, null);
  }

  public FlowRunner createFromFlowFile(final String flowName, final FlowWatcher watcher,
      final Integer pipeline) throws Exception {
    return createFromFlowFile(flowName, new EventCollectorListener(), new ExecutionOptions(),
        watcher, pipeline);
  }

  public FlowRunner createFromFlowFile(final String flowName,
      final EventCollectorListener eventCollector,
      final ExecutionOptions options, final FlowWatcher watcher, final Integer pipeline)
      throws Exception {
    final ExecutableFlow exFlow = FlowRunnerTestUtil
        .prepareExecDir(this.workingDir, this.projectDir, flowName, 1);
    exFlow.setDispatchMethod(DispatchMethod.POLL);
    if (watcher != null) {
      options.setPipelineLevel(pipeline);
      options.setPipelineExecutionId(watcher.getExecId());
    }
    // Add version set to executable flow
    exFlow.setVersionSet(createVersionSet());
    final FlowRunner runner = createFromExecutableFlow(eventCollector, exFlow, options,
        new HashMap<>(),
        new Props());
    runner.setFlowWatcher(watcher);
    return runner;
  }

  public FlowRunner createFromFlowMap(final String flowName, final String jobIdPrefix)
      throws Exception {
    return createFromFlowMap(flowName, jobIdPrefix, new ExecutionOptions());
  }

  public FlowRunner createFromFlowMap(final String flowName, final String jobIdPrefix,
      final ExecutionOptions options)
      throws Exception {
    return createFromFlowMap(flowName, jobIdPrefix, options, new Props());
  }

  public FlowRunner createFromFlowMap(final String flowName,
      final HashMap<String, String> flowParams) throws Exception {
    return createFromFlowMap(null, flowName, null, flowParams, new Props());
  }

  public FlowRunner createFromFlowMap(final String flowName, final ExecutionOptions options,
      final Map<String, String> flowParams, final Props azkabanProps) throws Exception {
    return createFromFlowMap(null, flowName, options, flowParams,
        azkabanProps);
  }

  public FlowRunner createFromFlowMap(final EventCollectorListener eventCollector,
      final String flowName, final ExecutionOptions options,
      final Map<String, String> flowParams, final Props azkabanProps)
      throws Exception {
    LOG.info("Creating a FlowRunner for flow '" + flowName + "'");
    final Flow flow = this.flowMap.get(flowName);
    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    return createFromExecutableFlow(eventCollector, exFlow, options, flowParams,
        azkabanProps);
  }

  public FlowRunner createFromFlowMap(final String flowName, final FailureAction action)
      throws Exception {
    final ExecutionOptions options = new ExecutionOptions();
    options.setFailureAction(action);
    return createFromFlowMap(flowName, options, new HashMap<>(), new Props());
  }

  private FlowRunner createFromFlowMap(final String flowName, final String jobIdPrefix,
      final ExecutionOptions options, final Props azkabanProps) throws Exception {
    final Map<String, String> flowParams = new HashMap<>();
    flowParams.put(InteractiveTestJob.JOB_ID_PREFIX, jobIdPrefix);
    return createFromFlowMap(new EventCollectorListener(), flowName, options, flowParams,
        azkabanProps);
  }

  private FlowRunner createFromExecutableFlow(final EventCollectorListener eventCollector,
      final ExecutableFlow exFlow, final ExecutionOptions options,
      final Map<String, String> flowParams, final Props azkabanProps)
      throws Exception {
    final int exId = id++;
    exFlow.setExecutionPath(this.workingDir.getPath());
    exFlow.setExecutionId(exId);
    if (options != null) {
      exFlow.setExecutionOptions(options);
      FlowUtils.applyDisabledJobs(options.getDisabledJobs(), exFlow);
    }
    exFlow.getExecutionOptions().addAllFlowParameters(flowParams);
    this.executorLoader.uploadExecutableFlow(exFlow);
    final MetricsManager metricsManager = new MetricsManager(new MetricRegistry());
    final CommonMetrics commonMetrics = new CommonMetrics(metricsManager);
    final ExecMetrics execMetrics = new ExecMetrics(metricsManager);
    final FlowRunner runner =
        new FlowRunner(exFlow, this.executorLoader, this.projectLoader,
            this.jobtypeManager, azkabanProps, null, mock(AlerterHolder.class), commonMetrics,
            execMetrics);
    if (eventCollector != null) {
      runner.addListener(eventCollector);
    }
    return runner;
  }

  public ExecutorLoader getExecutorLoader() {
    return this.executorLoader;
  }

  public void setExecutorLoader(final MockExecutorLoader executorLoader) {
    this.executorLoader = executorLoader;
  }

  public ProjectLoader getProjectLoader() {
    return this.projectLoader;
  }

  public Project getProject() {
    return this.project;
  }

  public VersionSet createVersionSet() {
    final String testJsonString1 = "{\"azkaban-base\":{\"version\":\"7.0.4\",\"path\":\"path1\","
        + "\"state\":\"ACTIVE\"},\"azkaban-config\":{\"version\":\"9.1.1\",\"path\":\"path2\","
        + "\"state\":\"ACTIVE\"},\"spark\":{\"version\":\"8.0\",\"path\":\"path3\","
        + "\"state\":\"ACTIVE\"}}";
    final String testMd5Hex1 = "43966138aebfdc4438520cc5cd2aefa8";
    return new VersionSet(testJsonString1, testMd5Hex1, 1);
  }
}

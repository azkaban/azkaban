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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.InteractiveTestJob;
import azkaban.flow.Flow;
import azkaban.jobtype.JobTypeManager;
import azkaban.jobtype.JobTypePluginSet;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TemporaryFolder;

public class FlowRunnerTestUtil {

  private static int id = 101;
  private final Map<String, Flow> flowMap;
  private final Project project;
  private final File workingDir;
  private final ExecutorLoader fakeExecutorLoader;
  private final JobTypeManager jobtypeManager;
  private final File projectDir;

  public FlowRunnerTestUtil(final String flowName, final TemporaryFolder temporaryFolder)
      throws Exception {

    this.projectDir = ExecutionsTestUtil.getFlowDir(flowName);
    this.workingDir = temporaryFolder.newFolder();
    this.project = new Project(1, "testProject");

    this.flowMap = FlowRunnerTestUtil
        .prepareProject(this.project, ExecutionsTestUtil.getFlowDir(flowName), this.workingDir);

    this.fakeExecutorLoader = mock(ExecutorLoader.class);
    when(this.fakeExecutorLoader.updateExecutableReference(anyInt(), anyLong())).thenReturn(true);

    Utils.initServiceProvider();
    JmxJobMBeanManager.getInstance().initialize(new Props());

    InteractiveTestJob.clearTestJobs();

    this.jobtypeManager = new JobTypeManager(null, null, this.getClass().getClassLoader());
    final JobTypePluginSet pluginSet = this.jobtypeManager.getJobTypePluginSet();
    pluginSet.addPluginClass("test", InteractiveTestJob.class);
  }

  /**
   * Initialize the project with the flow definitions stored in the given source directory. Also
   * copy the source directory to the working directory.
   *
   * @param project project to initialize
   * @param sourceDir the source dir
   * @param workingDir the working dir
   * @return the flow name to flow map
   * @throws ProjectManagerException the project manager exception
   * @throws IOException the io exception
   */
  public static Map<String, Flow> prepareProject(final Project project, final File sourceDir,
      final File workingDir)
      throws ProjectManagerException, IOException {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());
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
    project.setFlows(flowMap);
    FileUtils.copyDirectory(sourceDir, workingDir);
    return flowMap;
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

  public static void startThread(final FlowRunner runner) {
    new Thread(runner).start();
  }

  public FlowRunner createFromFlowFile(final EventCollectorListener eventCollector,
      final String flowName) throws Exception {
    return createFromFlowFile(flowName, eventCollector, new ExecutionOptions());
  }

  public FlowRunner createFromFlowFile(final String flowName,
      final EventCollectorListener eventCollector,
      final ExecutionOptions options) throws Exception {
    final ExecutableFlow exFlow = FlowRunnerTestUtil
        .prepareExecDir(this.workingDir, this.projectDir, flowName, 1);
    return createFromExecutableFlow(eventCollector, exFlow, options, new HashMap<>(), new Props());
  }

  public FlowRunner createFromFlowMap(final EventCollectorListener eventCollector,
      final String flowName, final String jobIdPrefix) throws Exception {
    return createFromFlowMap(eventCollector, flowName, jobIdPrefix,
        new ExecutionOptions(), new Props());
  }

  public FlowRunner createFromFlowMap(final EventCollectorListener eventCollector,
      final String flowName, final String jobIdPrefix, final ExecutionOptions options)
      throws Exception {
    return createFromFlowMap(eventCollector, flowName, jobIdPrefix,
        options, new Props());
  }

  public FlowRunner createFromFlowMap(final String flowName,
      final HashMap<String, String> flowParams) throws Exception {
    return createFromFlowMap(null, flowName, null, flowParams, new Props());
  }

  public FlowRunner createFromFlowMap(final EventCollectorListener eventCollector,
      final String flowName, final ExecutionOptions options,
      final Map<String, String> flowParams, final Props azkabanProps)
      throws Exception {
    final Flow flow = this.flowMap.get(flowName);
    final ExecutableFlow exFlow = new ExecutableFlow(this.project, flow);
    return createFromExecutableFlow(eventCollector, exFlow, options, flowParams, azkabanProps);
  }

  public FlowRunner createFromFlowMap(final EventCollectorListener eventCollector,
      final String flowName, final FailureAction action)
      throws Exception {
    final ExecutionOptions options = new ExecutionOptions();
    options.setFailureAction(action);
    return createFromFlowMap(eventCollector, flowName, options, new HashMap<>(), new Props());
  }

  private FlowRunner createFromFlowMap(final EventCollectorListener eventCollector,
      final String flowName, final String jobIdPrefix, final ExecutionOptions options,
      final Props azkabanProps)
      throws Exception {
    final Map<String, String> flowParams = new HashMap<>();
    flowParams.put(InteractiveTestJob.JOB_ID_PREFIX, jobIdPrefix);
    return createFromFlowMap(eventCollector, flowName, options, flowParams, azkabanProps);
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
    }
    exFlow.getExecutionOptions().addAllFlowParameters(flowParams);
    final FlowRunner runner =
        new FlowRunner(exFlow, this.fakeExecutorLoader, mock(ProjectLoader.class),
            this.jobtypeManager, azkabanProps, null);
    if (eventCollector != null) {
      runner.addListener(eventCollector);
    }
    return runner;
  }
}

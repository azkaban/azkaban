/*
* Copyright 2018 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.executor;

import azkaban.event.EventHandler;
import azkaban.project.Project;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import java.io.IOException;
import java.lang.Thread.State;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controls flow executions on web server. This module implements the polling model
 * in the new AZ dispatching design. It's injected only when azkaban.poll.model is configured to
 * true. It will ultimately replace ExecutorManager in the future.
 */
@Singleton
public class ExecutionController extends EventHandler implements ExecutorManagerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(ExecutionController.class);
  private static final Duration RECENTLY_FINISHED_LIFETIME = Duration.ofMinutes(10);
  private final ExecutorLoader executorLoader;
  private final ExecutorApiGateway apiGateway;

  @Inject
  ExecutionController(final ExecutorLoader executorLoader, final ExecutorApiGateway apiGateway) {
    this.executorLoader = executorLoader;
    this.apiGateway = apiGateway;
  }

  @Override
  public void setupExecutors() throws ExecutorManagerException {
    // Todo: deprecate this method
  }

  @Override
  public void disableQueueProcessorThread() {
    // Todo: deprecate this method
  }

  @Override
  public void enableQueueProcessorThread() {
    // Todo: deprecate this method
  }

  @Override
  public State getExecutorManagerThreadState() {
    // Todo: deprecate this method
    return State.RUNNABLE;
  }

  @Override
  public boolean isExecutorManagerThreadActive() {
    // Todo: deprecate this method
    return true;
  }

  @Override
  public long getLastExecutorManagerThreadCheckTime() {
    // Todo: deprecate this method
    return 1L;
  }

  @Override
  public Collection<Executor> getAllActiveExecutors() {
    // Todo: get the executor info from DB
    return Collections.emptyList();
  }

  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    return this.executorLoader.fetchExecutor(executorId);
  }

  @Override
  public Set<String> getPrimaryServerHosts() {
    // Todo: get the primary executor host info from DB
    return Collections.emptySet();
  }

  @Override
  public Set<String> getAllActiveExecutorServerHosts() {
    // Todo: get all executor host info from DB
    return Collections.emptySet();
  }

  /**
   * Gets a list of all the active (running flows and non-dispatched flows) executions for a given
   * project and flow from database. {@inheritDoc}
   */
  @Override
  public List<Integer> getRunningFlows(final int projectId, final String flowId) {
    // Todo: get running flows from DB
    return Collections.emptyList();
  }

  @Override
  public List<Pair<ExecutableFlow, Optional<Executor>>> getActiveFlowsWithExecutor()
      throws IOException {
    // Todo: get active flows with executor from DB
    return Collections.emptyList();
  }

  /**
   * Checks whether the given flow has an active (running, non-dispatched) execution from
   * database. {@inheritDoc}
   */
  @Override
  public boolean isFlowRunning(final int projectId, final String flowId) {
    // Todo: check DB to see if flow is running
    return true;
  }

  /**
   * Fetch ExecutableFlow from database. {@inheritDoc}
   */
  @Override
  public ExecutableFlow getExecutableFlow(final int execId)
      throws ExecutorManagerException {
    return this.executorLoader.fetchExecutableFlow(execId);
  }

  /**
   * Get all active (running, non-dispatched) flows from database. {@inheritDoc}
   */
  @Override
  public List<ExecutableFlow> getRunningFlows() {
    // Todo: get running flows from DB
    return Collections.emptyList();
  }

  @Override
  public List<ExecutableFlow> getRecentlyFinishedFlows() {
    List<ExecutableFlow> flows = new ArrayList<>();
    try {
      flows = this.executorLoader.fetchRecentlyFinishedFlows(
          RECENTLY_FINISHED_LIFETIME);
    } catch (final ExecutorManagerException e) {
      logger.error("Failed to fetch recently finished flows.", e);
    }
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final int skip, final int size)
      throws ExecutorManagerException {
    final List<ExecutableFlow> flows = this.executorLoader.fetchFlowHistory(skip, size);
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final String flowIdContains,
      final int skip, final int size) throws ExecutorManagerException {
    final List<ExecutableFlow> flows =
        this.executorLoader.fetchFlowHistory(null, '%' + flowIdContains + '%', null,
            0, -1, -1, skip, size);
    return flows;
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final String projContain,
      final String flowContain, final String userContain, final int status, final long begin,
      final long end,
      final int skip, final int size) throws ExecutorManagerException {
    final List<ExecutableFlow> flows =
        this.executorLoader.fetchFlowHistory(projContain, flowContain, userContain,
            status, begin, end, skip, size);
    return flows;
  }

  @Override
  public List<ExecutableJobInfo> getExecutableJobs(final Project project,
      final String jobId, final int skip, final int size) throws ExecutorManagerException {
    final List<ExecutableJobInfo> nodes =
        this.executorLoader.fetchJobHistory(project.getId(), jobId, skip, size);
    return nodes;
  }

  @Override
  public int getNumberOfJobExecutions(final Project project, final String jobId)
      throws ExecutorManagerException {
    return this.executorLoader.fetchNumExecutableNodes(project.getId(), jobId);
  }

  @Override
  public LogData getExecutableFlowLog(final ExecutableFlow exFlow, final int offset,
      final int length) throws ExecutorManagerException {
    // Todo: get the flow log from executor if the flow is running, else get it from DB.
    return new LogData(0, 0, "dummy");
  }

  @Override
  public LogData getExecutionJobLog(final ExecutableFlow exFlow, final String jobId,
      final int offset, final int length, final int attempt) throws ExecutorManagerException {
    // Todo: get the job log from executor if the flow is running, else get it from DB.
    return new LogData(0, 0, "dummy");
  }

  @Override
  public List<Object> getExecutionJobStats(final ExecutableFlow exFlow, final String jobId,
      final int attempt) throws ExecutorManagerException {
    // Todo: get execution job status from executor if the flow is running, else get if from DB.
    return Collections.emptyList();
  }

  @Override
  public String getJobLinkUrl(final ExecutableFlow exFlow, final String jobId, final int attempt) {
    // Todo: deprecate this method
    return null;
  }

  /**
   * If a flow was dispatched to an executor, cancel by calling Executor. Else if it's still
   * queued in database, remove it from database queue and finalize. {@inheritDoc}
   */
  @Override
  public void cancelFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    // Todo: call executor to cancel the flow if it's running or remove from DB queue if it
    // hasn't started
  }

  @Override
  public void resumeFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    // Todo: call executor to resume the flow
  }

  @Override
  public void pauseFlow(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    // Todo: call executor to pause the flow
  }

  @Override
  public void retryFailures(final ExecutableFlow exFlow, final String userId)
      throws ExecutorManagerException {
    // Todo: call executor to retry failed flows
  }

  /**
   * When a flow is submitted, insert a new execution into the database queue. {@inheritDoc}
   */
  @Override
  public String submitExecutableFlow(final ExecutableFlow exflow, final String userId)
      throws ExecutorManagerException {
    // Todo: insert the execution to DB queue
    return null;
  }

  @Override
  public Map<String, Object> callExecutorStats(final int executorId, final String action,
      final Pair<String, String>... params) throws IOException, ExecutorManagerException {
    final Executor executor = fetchExecutor(executorId);
    final List<Pair<String, String>> paramList = new ArrayList<>();

    if (params != null) {
      paramList.addAll(Arrays.asList(params));
    }

    paramList.add(new Pair<>(ConnectorParams.ACTION_PARAM, action));

    return this.apiGateway.callForJsonObjectMap(executor.getHost(), executor.getPort(),
        "/stats", paramList);
  }

  @Override
  public Map<String, Object> callExecutorJMX(final String hostPort, final String action,
      final String mBean) throws IOException {
    final List<Pair<String, String>> paramList =
        new ArrayList<>();

    paramList.add(new Pair<>(action, ""));
    if (mBean != null) {
      paramList.add(new Pair<>(ConnectorParams.JMX_MBEAN, mBean));
    }

    final String[] hostPortSplit = hostPort.split(":");
    return this.apiGateway.callForJsonObjectMap(hostPortSplit[0],
        Integer.valueOf(hostPortSplit[1]), "/jmx", paramList);
  }


  @Override
  public void shutdown() {
    //Todo: shutdown any thread that is running
  }

  @Override
  public int getExecutableFlows(final int projectId, final String flowId, final int from,
      final int length, final List<ExecutableFlow> outputList)
      throws ExecutorManagerException {
    final List<ExecutableFlow> flows =
        this.executorLoader.fetchFlowHistory(projectId, flowId, from, length);
    outputList.addAll(flows);
    return this.executorLoader.fetchNumExecutableFlows(projectId, flowId);
  }

  @Override
  public List<ExecutableFlow> getExecutableFlows(final int projectId, final String flowId,
      final int from, final int length, final Status status) throws ExecutorManagerException {
    return this.executorLoader.fetchFlowHistory(projectId, flowId, from, length,
        status);
  }

}

/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.executor;

import azkaban.project.Project;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import java.io.IOException;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ExecutorManagerAdapter {

  public boolean isFlowRunning(int projectId, String flowId);

  public ExecutableFlow getExecutableFlow(int execId)
      throws ExecutorManagerException;

  public List<Integer> getRunningFlows(int projectId, String flowId);

  public List<ExecutableFlow> getRunningFlows();

  public long getQueuedFlowSize();

  /**
   * <pre>
   * Returns All running with executors and queued flows
   * Note, returns empty list if there isn't any running or queued flows
   * </pre>
   */
  public List<Pair<ExecutableFlow, Optional<Executor>>> getActiveFlowsWithExecutor()
      throws IOException;

  public List<ExecutableFlow> getRecentlyFinishedFlows();

  public List<ExecutableFlow> getExecutableFlows(int skip, int size)
      throws ExecutorManagerException;

  public List<ExecutableFlow> getExecutableFlows(String flowIdContains,
      int skip, int size) throws ExecutorManagerException;

  public List<ExecutableFlow> getExecutableFlows(String projContain,
      String flowContain, String userContain, int status, long begin, long end,
      int skip, int size) throws ExecutorManagerException;

  public int getExecutableFlows(int projectId, String flowId, int from,
      int length, List<ExecutableFlow> outputList)
      throws ExecutorManagerException;

  public List<ExecutableFlow> getExecutableFlows(int projectId, String flowId,
      int from, int length, Status status) throws ExecutorManagerException;

  public List<ExecutableJobInfo> getExecutableJobs(Project project,
      String jobId, int skip, int size) throws ExecutorManagerException;

  public int getNumberOfJobExecutions(Project project, String jobId)
      throws ExecutorManagerException;

  public LogData getExecutableFlowLog(ExecutableFlow exFlow, int offset,
      int length) throws ExecutorManagerException;

  public LogData getExecutionJobLog(ExecutableFlow exFlow, String jobId,
      int offset, int length, int attempt) throws ExecutorManagerException;

  public List<Object> getExecutionJobStats(ExecutableFlow exflow, String jobId,
      int attempt) throws ExecutorManagerException;

  public Map<String, String> getExternalJobLogUrls(ExecutableFlow exFlow, String jobId,
      int attempt);

  public void cancelFlow(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException;

  public void resumeFlow(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException;

  public void pauseFlow(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException;

  public void retryFailures(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException;

  public String submitExecutableFlow(ExecutableFlow exflow, String userId)
      throws ExecutorManagerException;

  public Map<String, String> doRampActions(List<Map<String, Object>> rampAction)
      throws ExecutorManagerException;

  /**
   * Manage servlet call for stats servlet in Azkaban execution server Action can take any of the
   * following values <ul> <li>{@link azkaban.executor.ConnectorParams#STATS_SET_REPORTINGINTERVAL}<li>
   * <li>{@link azkaban.executor.ConnectorParams#STATS_SET_CLEANINGINTERVAL}<li> <li>{@link
   * azkaban.executor.ConnectorParams#STATS_SET_MAXREPORTERPOINTS}<li> <li>{@link
   * azkaban.executor.ConnectorParams#STATS_GET_ALLMETRICSNAME}<li> <li>{@link
   * azkaban.executor.ConnectorParams#STATS_GET_METRICHISTORY}<li> <li>{@link
   * azkaban.executor.ConnectorParams#STATS_SET_ENABLEMETRICS}<li> <li>{@link
   * azkaban.executor.ConnectorParams#STATS_SET_DISABLEMETRICS}<li> </ul>
   */
  public Map<String, Object> callExecutorStats(int executorId, String action,
      Pair<String, String>... param) throws IOException, ExecutorManagerException;

  public Map<String, Object> callExecutorJMX(String hostPort, String action,
      String mBean) throws IOException;

  public void start() throws ExecutorManagerException;

  public void shutdown();

  public Set<String> getAllActiveExecutorServerHosts();

  public State getExecutorManagerThreadState();

  public boolean isExecutorManagerThreadActive();

  public long getLastExecutorManagerThreadCheckTime();

  public Set<? extends String> getPrimaryServerHosts();

  /**
   * Returns a collection of all the active executors maintained by active executors
   */
  public Collection<Executor> getAllActiveExecutors();

  /**
   * <pre>
   * Fetch executor from executors with a given executorId
   * Note:
   * 1. throws an Exception in case of a SQL issue
   * 2. return null when no executor is found with the given executorId
   * </pre>
   */
  public Executor fetchExecutor(int executorId) throws ExecutorManagerException;

  /**
   * <pre>
   * Setup activeExecutors using azkaban.properties and database executors
   * Note:
   * 1. If azkaban.use.multiple.executors is set true, this method will
   *    load all active executors
   * 2. In local mode, If a local executor is specified and it is missing from db,
   *    this method add local executor as active in DB
   * 3. In local mode, If a local executor is specified and it is marked inactive in db,
   *    this method will convert local executor as active in DB
   * </pre>
   */
  public void setupExecutors() throws ExecutorManagerException;

  /**
   * Enable flow dispatching in QueueProcessor
   */
  public void enableQueueProcessorThread() throws ExecutorManagerException;

  /**
   * Disable flow dispatching in QueueProcessor
   */
  public void disableQueueProcessorThread() throws ExecutorManagerException;
}

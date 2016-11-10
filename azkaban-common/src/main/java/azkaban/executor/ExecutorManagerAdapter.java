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

import java.io.IOException;
import java.lang.Thread.State;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.project.Project;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;

public interface ExecutorManagerAdapter {

  public static final String LOCAL_MODE = "local";
  public static final String REMOTE_MODE = "remote";

  public static final String REMOTE_EXECUTOR_MANAGER_HOST =
      "remote.executor.manager.host";
  public static final String REMOTE_EXECUTOR_MANAGER_PORT =
      "remote.executor.manager.port";
  public static final String REMOTE_EXECUTOR_MANAGER_URL = "/executormanager";

  public static final String ACTION_GET_FLOW_LOG = "getFlowLog";
  public static final String ACTION_GET_JOB_LOG = "getJobLog";
  public static final String ACTION_CANCEL_FLOW = "cancelFlow";
  public static final String ACTION_SUBMIT_FLOW = "submitFlow";
  public static final String ACTION_RESUME_FLOW = "resumeFlow";
  public static final String ACTION_PAUSE_FLOW = "pauseFlow";
  public static final String ACTION_MODIFY_EXECUTION = "modifyExecution";
  public static final String ACTION_UPDATE = "update";
  public static final String ACTION_GET_JMX = "getJMX";

  public static final String COMMAND_MODIFY_PAUSE_JOBS = "modifyPauseJobs";
  public static final String COMMAND_MODIFY_RESUME_JOBS = "modifyResumeJobs";
  public static final String COMMAND_MODIFY_RETRY_FAILURES =
      "modifyRetryFailures";
  public static final String COMMAND_MODIFY_RETRY_JOBS = "modifyRetryJobs";
  public static final String COMMAND_MODIFY_DISABLE_JOBS = "modifyDisableJobs";
  public static final String COMMAND_MODIFY_ENABLE_JOBS = "modifyEnableJobs";
  public static final String COMMAND_MODIFY_CANCEL_JOBS = "modifyCancelJobs";

  public static final String INFO_JMX_TYPE = "jmxType";
  public static final String INFO_JMX_DATA = "jmxData";
  public static final String INFO_ACTION = "action";
  public static final String INFO_TYPE = "type";
  public static final String INFO_EXEC_ID = "execId";
  public static final String INFO_EXEC_FLOW_JSON = "execFlowJson";
  public static final String INFO_PROJECT_ID = "projectId";
  public static final String INFO_FLOW_NAME = "flowName";
  public static final String INFO_JOB_NAME = "jobName";
  public static final String INFO_OFFSET = "offset";
  public static final String INFO_LENGTH = "length";
  public static final String INFO_ATTEMPT = "attempt";
  public static final String INFO_MODIFY_JOB_IDS = "modifyJobIds";
  public static final String INFO_MODIFY_COMMAND = "modifyCommand";
  public static final String INFO_MESSAGE = "message";
  public static final String INFO_ERROR = "error";
  public static final String INFO_UPDATE_TIME_LIST = "updateTimeList";
  public static final String INFO_EXEC_ID_LIST = "execIdList";
  public static final String INFO_UPDATES = "updates";
  public static final String INFO_USER_ID = "userId";
  public static final String INFO_LOG = "logData";

  public boolean isFlowRunning(int projectId, String flowId);

  public ExecutableFlow getExecutableFlow(int execId)
      throws ExecutorManagerException;

  public List<Integer> getRunningFlows(int projectId, String flowId);

  public List<ExecutableFlow> getRunningFlows() throws IOException;

  /**
   * <pre>
   * Returns All running with executors and queued flows
   * Note, returns empty list if there isn't any running or queued flows
   * </pre>
   *
   * @return
   * @throws IOException
   */
  public List<Pair<ExecutableFlow, Executor>> getActiveFlowsWithExecutor()
    throws IOException;

  public List<ExecutableFlow> getRecentlyFinishedFlows();

  public List<ExecutableFlow> getExecutableFlows(Project project,
      String flowId, int skip, int size) throws ExecutorManagerException;

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

  public int getNumberOfExecutions(Project project, String flowId)
      throws ExecutorManagerException;

  public LogData getExecutableFlowLog(ExecutableFlow exFlow, int offset,
      int length) throws ExecutorManagerException;

  public LogData getExecutionJobLog(ExecutableFlow exFlow, String jobId,
      int offset, int length, int attempt) throws ExecutorManagerException;

  public List<Object> getExecutionJobStats(ExecutableFlow exflow, String jobId,
      int attempt) throws ExecutorManagerException;

  public JobMetaData getExecutionJobMetaData(ExecutableFlow exFlow,
      String jobId, int offset, int length, int attempt)
      throws ExecutorManagerException;

  public void cancelFlow(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException;

  public void resumeFlow(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException;

  public void pauseFlow(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException;

  public void pauseExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException;

  public void resumeExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException;

  public void retryFailures(ExecutableFlow exFlow, String userId)
      throws ExecutorManagerException;

  public void retryExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException;

  public void disableExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException;

  public void enableExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException;

  public void cancelExecutingJobs(ExecutableFlow exFlow, String userId,
      String... jobIds) throws ExecutorManagerException;

  public String submitExecutableFlow(ExecutableFlow exflow, String userId)
      throws ExecutorManagerException;

  /**
   * Manage servlet call for stats servlet in Azkaban execution server
   * Action can take any of the following values
   * <ul>
   * <li>{@link azkaban.executor.ConnectorParams#STATS_SET_REPORTINGINTERVAL}<li>
   * <li>{@link azkaban.executor.ConnectorParams#STATS_SET_CLEANINGINTERVAL}<li>
   * <li>{@link azkaban.executor.ConnectorParams#STATS_SET_MAXREPORTERPOINTS}<li>
   * <li>{@link azkaban.executor.ConnectorParams#STATS_GET_ALLMETRICSNAME}<li>
   * <li>{@link azkaban.executor.ConnectorParams#STATS_GET_METRICHISTORY}<li>
   * <li>{@link azkaban.executor.ConnectorParams#STATS_SET_ENABLEMETRICS}<li>
   * <li>{@link azkaban.executor.ConnectorParams#STATS_SET_DISABLEMETRICS}<li>
   * </ul>
   * @throws ExecutorManagerException
   */
  public Map<String, Object> callExecutorStats(int executorId, String action,
    Pair<String, String>... param) throws IOException, ExecutorManagerException;

  public Map<String, Object> callExecutorJMX(String hostPort, String action,
      String mBean) throws IOException;

  public void shutdown();

  public Set<String> getAllActiveExecutorServerHosts();

  public State getExecutorManagerThreadState();

  public boolean isExecutorManagerThreadActive();

  public long getLastExecutorManagerThreadCheckTime();

  public Set<? extends String> getPrimaryServerHosts();

  /**
   * Returns a collection of all the active executors maintained by active
   * executors
   *
   * @return
   */
  public Collection<Executor> getAllActiveExecutors();

  /**
   * <pre>
   * Fetch executor from executors with a given executorId
   * Note:
   * 1. throws an Exception in case of a SQL issue
   * 2. return null when no executor is found with the given executorId
   * </pre>
   *
   * @throws ExecutorManagerException
   *
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
   *
   * @throws ExecutorManagerException
   */
   public void setupExecutors() throws ExecutorManagerException;

   /**
    * Enable flow dispatching in QueueProcessor
    *
    * @throws ExecutorManagerException
    */
   public void enableQueueProcessorThread() throws ExecutorManagerException;

   /**
    * Disable flow dispatching in QueueProcessor
    *
    * @throws ExecutorManagerException
    */
   public void disableQueueProcessorThread() throws ExecutorManagerException;
}

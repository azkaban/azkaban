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

package azkaban.executor;

import azkaban.database.AbstractJdbcLoader;
import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

@Singleton
public class JdbcExecutorLoader extends AbstractJdbcLoader implements
    ExecutorLoader {
  private static final Logger logger = Logger
      .getLogger(JdbcExecutorLoader.class);
  private final ExecutionFlowDao executionFlowDao;
  private final ExecutorDao executorDao;
  private final ExecutionJobDao executionJobDao;
  private final ExecutionLogsDao executionLogsDao;
  private final ExecutorEventsDao executorEventsDao;
  private final ActiveExecutingFlowsDao activeExecutingFlowsDao;
  private final FetchActiveFlowDao fetchActiveFlowDao;
  private EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  public JdbcExecutorLoader(final Props props, final CommonMetrics commonMetrics,
                            final ExecutionFlowDao executionFlowDao,
                            final ExecutorDao executorDao,
                            final ExecutionJobDao executionJobDao,
                            final ExecutionLogsDao executionLogsDao,
                            final ExecutorEventsDao executorEventsDao,
                            final ActiveExecutingFlowsDao activeExecutingFlowsDao,
                            final FetchActiveFlowDao fetchActiveFlowDao) {
    super(props, commonMetrics);
    this.executionFlowDao = executionFlowDao;
    this.executorDao = executorDao;
    this.executionJobDao = executionJobDao;
    this.executionLogsDao= executionLogsDao;
    this.executorEventsDao = executorEventsDao;
    this.activeExecutingFlowsDao = activeExecutingFlowsDao;
    this.fetchActiveFlowDao = fetchActiveFlowDao;
  }

  public EncodingType getDefaultEncodingType() {
    return this.defaultEncodingType;
  }

  public void setDefaultEncodingType(final EncodingType defaultEncodingType) {
    this.defaultEncodingType = defaultEncodingType;
  }

  @Override
  public synchronized void uploadExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    this.executionFlowDao.uploadExecutableFlow(flow);
  }

  @Override
  public void updateExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    this.executionFlowDao.updateExecutableFlow(flow);
  }

  @Override
  public ExecutableFlow fetchExecutableFlow(final int id)
      throws ExecutorManagerException {
    return this.executionFlowDao.fetchExecutableFlow(id);
  }

  /**
   *
   * {@inheritDoc}
   * @see azkaban.executor.ExecutorLoader#fetchQueuedFlows()
   */
  @Override
  public List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows()
    throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchQueuedExecutableFlows flowHandler = new FetchQueuedExecutableFlows();

    try {
      final List<Pair<ExecutionReference, ExecutableFlow>> flows =
        runner.query(FetchQueuedExecutableFlows.FETCH_QUEUED_EXECUTABLE_FLOW,
          flowHandler);
      return flows;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  /**
   * maxAge indicates how long finished flows are shown in Recently Finished flow page.
   */
  @Override
  public List<ExecutableFlow> fetchRecentlyFinishedFlows(final Duration maxAge)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchRecentlyFinishedFlows flowHandler = new FetchRecentlyFinishedFlows();

    try {
      final List<ExecutableFlow> flows =
          runner.query(FetchRecentlyFinishedFlows.FETCH_RECENTLY_FINISHED_FLOW,
              flowHandler, System.currentTimeMillis() - maxAge.toMillis(),
              Status.SUCCEEDED.getNumVal(), Status.KILLED.getNumVal(),
              Status.FAILED.getNumVal());
      return flows;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching recently finished flows", e);
    }
  }

  @Override
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {

    return this.fetchActiveFlowDao.fetchActiveFlows();
  }

  @Override
  public Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(final int execId)
      throws ExecutorManagerException {

    return this.fetchActiveFlowDao.fetchActiveFlowByExecId(execId);
  }

  @Override
  public int fetchNumExecutableFlows() throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    final IntHandler intHandler = new IntHandler();
    try {
      final int count = runner.query(IntHandler.NUM_EXECUTIONS, intHandler);
      return count;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public int fetchNumExecutableFlows(final int projectId, final String flowId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    final IntHandler intHandler = new IntHandler();
    try {
      final int count =
          runner.query(IntHandler.NUM_FLOW_EXECUTIONS, intHandler, projectId,
              flowId);
      return count;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public int fetchNumExecutableNodes(final int projectId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    final IntHandler intHandler = new IntHandler();
    try {
      final int count =
          runner.query(IntHandler.NUM_JOB_EXECUTIONS, intHandler, projectId,
              jobId);
      return count;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
                                               final int skip, final int num) throws ExecutorManagerException {
    return this.executionFlowDao.fetchFlowHistory(projectId, flowId, skip, num);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
                                               final int skip, final int num, final Status status) throws ExecutorManagerException {
    return this.executionFlowDao.fetchFlowHistory(projectId, flowId, skip, num, status);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int skip, final int num)
      throws ExecutorManagerException {
    return this.executionFlowDao.fetchFlowHistory(skip,num);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final String projContain,
                                               final String flowContains, final String userNameContains, final int status, final long startTime,
                                               final long endTime, final int skip, final int num) throws ExecutorManagerException {
    return this.executionFlowDao.fetchFlowHistory(projContain, flowContains,
        userNameContains, status, startTime, endTime, skip, num);
  }

  @Override
  public void addActiveExecutableReference(final ExecutionReference reference)
      throws ExecutorManagerException {

    this.activeExecutingFlowsDao.addActiveExecutableReference(reference);
  }

  @Override
  public void removeActiveExecutableReference(final int execid)
      throws ExecutorManagerException {

    this.activeExecutingFlowsDao.removeActiveExecutableReference(execid);
  }

  @Override
  public boolean updateExecutableReference(final int execId, final long updateTime)
      throws ExecutorManagerException {

    // Should be 1.
    return this.activeExecutingFlowsDao.updateExecutableReference(execId, updateTime);
  }

  @Override
  public void uploadExecutableNode(final ExecutableNode node, final Props inputProps)
      throws ExecutorManagerException {

    this.executionJobDao.uploadExecutableNode(node, inputProps);
  }

  @Override
  public void updateExecutableNode(final ExecutableNode node)
      throws ExecutorManagerException {

    this.executionJobDao.updateExecutableNode(node);
  }

  @Override
  public List<ExecutableJobInfo> fetchJobInfoAttempts(final int execId, final String jobId)
      throws ExecutorManagerException {

    return this.executionJobDao.fetchJobInfoAttempts(execId, jobId);
  }

  @Override
  public ExecutableJobInfo fetchJobInfo(final int execId, final String jobId, final int attempts)
      throws ExecutorManagerException {

    return this.executionJobDao.fetchJobInfo(execId, jobId, attempts);
  }

  @Override
  public Props fetchExecutionJobInputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    return this.executionJobDao.fetchExecutionJobInputProps(execId, jobId);
  }

  @Override
  public Props fetchExecutionJobOutputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    return this.executionJobDao.fetchExecutionJobOutputProps(execId, jobId);
  }

  @Override
  public Pair<Props, Props> fetchExecutionJobProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    return this.executionJobDao.fetchExecutionJobProps(execId, jobId);
  }

  @Override
  public List<ExecutableJobInfo> fetchJobHistory(final int projectId, final String jobId,
                                                 final int skip, final int size) throws ExecutorManagerException {

    return this.executionJobDao.fetchJobHistory(projectId, jobId, skip, size);
  }

  @Override
  public LogData fetchLogs(final int execId, final String name, final int attempt, final int startByte,
                           final int length) throws ExecutorManagerException {

    return this.executionLogsDao.fetchLogs(execId, name, attempt, startByte, length);
  }

  @Override
  public List<Object> fetchAttachments(final int execId, final String jobId, final int attempt)
      throws ExecutorManagerException {

    return this.executionJobDao.fetchAttachments(execId, jobId, attempt);
  }

  @Override
  public void uploadLogFile(final int execId, final String name, final int attempt, final File... files)
      throws ExecutorManagerException {
    this.executionLogsDao.uploadLogFile(execId, name, attempt, files);
  }

  private void uploadLogFile(final Connection connection, final int execId, final String name,
                             final int attempt, final File[] files, final EncodingType encType)
      throws ExecutorManagerException, IOException {
    // 50K buffer... if logs are greater than this, we chunk.
    // However, we better prevent large log files from being uploaded somehow
    this.executionLogsDao.uploadLogFile(connection, execId, name, attempt, files, encType);
  }

  @Override
  public void uploadAttachmentFile(final ExecutableNode node, final File file)
      throws ExecutorManagerException {
    this.executionJobDao.uploadAttachmentFile(node, file);
  }

  private Connection getConnection() throws ExecutorManagerException {
    Connection connection = null;
    try {
      connection = super.getDBConnection(false);
    } catch (final Exception e) {
      DbUtils.closeQuietly(connection);
      throw new ExecutorManagerException("Error getting DB connection.", e);
    }
    return connection;
  }


  /**
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchActiveExecutors()
   */
  @Override
  public List<Executor> fetchAllExecutors() throws ExecutorManagerException {
    return this.executorDao.fetchAllExecutors();
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchActiveExecutors()
   */
  @Override
  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    return this.executorDao.fetchActiveExecutors();
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchExecutor(java.lang.String, int)
   */
  @Override
  public Executor fetchExecutor(final String host, final int port)
    throws ExecutorManagerException {
    return this.executorDao.fetchExecutor(host, port);
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchExecutor(int)
   */
  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    return this.executorDao.fetchExecutor(executorId);
  }

  /**
   * {@inheritDoc}
   *
   */
  @Override
  public void updateExecutor(final Executor executor) throws ExecutorManagerException {
    this.executorDao.updateExecutor(executor);
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#addExecutor(java.lang.String, int)
   */
  @Override
  public Executor addExecutor(final String host, final int port)
    throws ExecutorManagerException {
    return this.executorDao.addExecutor(host, port);
  }


  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#removeExecutor(String, int)
   */
  @Override
  public void removeExecutor(final String host, final int port) throws ExecutorManagerException {
    this.executorDao.removeExecutor(host, port);
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#postExecutorEvent(azkaban.executor.Executor,
   *      azkaban.executor.ExecutorLogEvent.EventType, java.lang.String,
   *      java.lang.String)
   */
  @Override
  public void postExecutorEvent(final Executor executor, final EventType type, final String user,
                                final String message) throws ExecutorManagerException{

    this.executorEventsDao.postExecutorEvent(executor, type, user, message);
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#getExecutorEvents(azkaban.executor.Executor,
   *      int, int)
   */
  @Override
  public List<ExecutorLogEvent> getExecutorEvents(final Executor executor, final int num,
                                                  final int offset) throws ExecutorManagerException {

    return this.executorEventsDao.getExecutorEvents(executor, num, offset);
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#assignExecutor(int, int)
   */
  @Override
  public void assignExecutor(final int executorId, final int executionId)
    throws ExecutorManagerException {
    final String UPDATE =
      "UPDATE execution_flows SET executor_id=? where exec_id=?";

    final QueryRunner runner = createQueryRunner();
    try {
      final Executor executor = fetchExecutor(executorId);
      if (executor == null) {
        throw new ExecutorManagerException(String.format(
          "Failed to assign non-existent executor Id: %d to execution : %d  ",
          executorId, executionId));
      }

      final int rows = runner.update(UPDATE, executorId, executionId);
      if (rows == 0) {
        throw new ExecutorManagerException(String.format(
          "Failed to assign executor Id: %d to non-existent execution : %d  ",
          executorId, executionId));
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating executor id "
        + executorId, e);
    }
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchExecutorByExecutionId(int)
   */
  @Override
  public Executor fetchExecutorByExecutionId(final int executionId)
    throws ExecutorManagerException {
    return this.executorDao.fetchExecutorByExecutionId(executionId);
  }

  @Override
  public int removeExecutionLogsByTime(final long millis)
      throws ExecutorManagerException {

    return this.executionLogsDao.removeExecutionLogsByTime(millis);
  }

  /**
   *
   * {@inheritDoc}
   * @see azkaban.executor.ExecutorLoader#unassignExecutor(int)
   */
  @Override
  public void unassignExecutor(final int executionId) throws ExecutorManagerException {
    final String UPDATE =
      "UPDATE execution_flows SET executor_id=NULL where exec_id=?";

    final QueryRunner runner = createQueryRunner();
    try {
      final int rows = runner.update(UPDATE, executionId);
      if (rows == 0) {
        throw new ExecutorManagerException(String.format(
          "Failed to unassign executor for execution : %d  ", executionId));
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating execution id "
        + executionId, e);
    }
  }

  private static class LastInsertID implements ResultSetHandler<Long> {
    private static final String LAST_INSERT_ID = "SELECT LAST_INSERT_ID()";

    @Override
    public Long handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return -1L;
      }
      final long id = rs.getLong(1);
      return id;
    }
  }

  /**
   * JDBC ResultSetHandler to fetch queued executions
   */
  private static class FetchQueuedExecutableFlows implements
    ResultSetHandler<List<Pair<ExecutionReference, ExecutableFlow>>> {
    // Select queued unassigned flows
    private static final String FETCH_QUEUED_EXECUTABLE_FLOW =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows"
            + " Where executor_id is NULL AND status = "
            + Status.PREPARING.getNumVal();

    @Override
    public List<Pair<ExecutionReference, ExecutableFlow>> handle(final ResultSet rs)
      throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final List<Pair<ExecutionReference, ExecutableFlow>> execFlows =
        new ArrayList<>();
      do {
        final int id = rs.getInt(1);
        final int encodingType = rs.getInt(2);
        final byte[] data = rs.getBytes(3);

        if (data == null) {
          logger.error("Found a flow with empty data blob exec_id: " + id);
        } else {
          final EncodingType encType = EncodingType.fromInteger(encodingType);
          final Object flowObj;
          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              final String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              final String jsonString = new String(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            }

            final ExecutableFlow exFlow =
              ExecutableFlow.createExecutableFlowFromObject(flowObj);
            final ExecutionReference ref = new ExecutionReference(id);
            execFlows.add(new Pair<>(ref, exFlow));
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }

  private static class FetchRecentlyFinishedFlows implements
    ResultSetHandler<List<ExecutableFlow>> {
    // Execution_flows table is already indexed by end_time
    private static final String FETCH_RECENTLY_FINISHED_FLOW =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE end_time > ? AND status IN (?, ?, ?)";

    @Override
    public List<ExecutableFlow> handle(
        final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final List<ExecutableFlow> execFlows = new ArrayList<>();
      do {
        final int id = rs.getInt(1);
        final int encodingType = rs.getInt(2);
        final byte[] data = rs.getBytes(3);

        if (data != null) {
          final EncodingType encType = EncodingType.fromInteger(encodingType);
          final Object flowObj;
          try {
            if (encType == EncodingType.GZIP) {
              final String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              final String jsonString = new String(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            }

            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(flowObj);

            execFlows.add(exFlow);
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }

  private static class IntHandler implements ResultSetHandler<Integer> {
    private static final String NUM_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_flows";
    private static final String NUM_FLOW_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_flows WHERE project_id=? AND flow_id=?";
    private static final String NUM_JOB_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_jobs WHERE project_id=? AND job_id=?";
    private static final String FETCH_EXECUTOR_ID =
        "SELECT executor_id FROM execution_flows WHERE exec_id=?";

    @Override
    public Integer handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }

}

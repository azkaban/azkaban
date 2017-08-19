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
import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

@Singleton
public class JdbcExecutorLoader extends AbstractJdbcLoader implements
    ExecutorLoader {
  private static final Logger logger = Logger
      .getLogger(JdbcExecutorLoader.class);
  private final ExecutionFlowDao executionFlowDao;
  private final ExecutorDao executorDao;
  private final ExecutionJobDao executionJobDao;
  private EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  public JdbcExecutorLoader(final Props props, final CommonMetrics commonMetrics,
                            final ExecutionFlowDao executionFlowDao,
                            final ExecutorDao executorDao,
                            final ExecutionJobDao executionJobDao) {
    super(props, commonMetrics);
    this.executionFlowDao = executionFlowDao;
    this.executorDao = executorDao;
    this.executionJobDao = executionJobDao;
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
    final QueryRunner runner = createQueryRunner();
    final FetchActiveExecutableFlows flowHandler = new FetchActiveExecutableFlows();

    try {
      final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> properties =
          runner.query(FetchActiveExecutableFlows.FETCH_ACTIVE_EXECUTABLE_FLOW,
              flowHandler);
      return properties;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(final int execId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchActiveExecutableFlowByExecId flowHandler = new FetchActiveExecutableFlowByExecId();

    try {
      final List<Pair<ExecutionReference, ExecutableFlow>> flows =
          runner.query(FetchActiveExecutableFlowByExecId.FETCH_ACTIVE_EXECUTABLE_FLOW_BY_EXECID,
              flowHandler, execId);
      if(flows.isEmpty()) {
        return null;
      }
      else {
        return flows.get(0);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows by exec id", e);
    }
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
    final String INSERT =
        "INSERT INTO active_executing_flows "
            + "(exec_id, update_time) values (?,?)";
    final QueryRunner runner = createQueryRunner();

    try {
      runner.update(INSERT, reference.getExecId(), reference.getUpdateTime());
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error updating active flow reference " + reference.getExecId(), e);
    }
  }

  @Override
  public void removeActiveExecutableReference(final int execid)
      throws ExecutorManagerException {
    final String DELETE = "DELETE FROM active_executing_flows WHERE exec_id=?";

    final QueryRunner runner = createQueryRunner();
    try {
      runner.update(DELETE, execid);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execid, e);
    }
  }

  @Override
  public boolean updateExecutableReference(final int execId, final long updateTime)
      throws ExecutorManagerException {
    final String DELETE =
        "UPDATE active_executing_flows set update_time=? WHERE exec_id=?";

    final QueryRunner runner = createQueryRunner();
    int updateNum = 0;
    try {
      updateNum = runner.update(DELETE, updateTime, execId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execId, e);
    }

    // Should be 1.
    return updateNum > 0;
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
    final QueryRunner runner = createQueryRunner();

    final FetchLogsHandler handler =
        new FetchLogsHandler(startByte, length + startByte);
    try {
      final LogData result =
          runner.query(FetchLogsHandler.FETCH_LOGS, handler, execId, name,
              attempt, startByte, startByte + length);
      return result;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching logs " + execId
          + " : " + name, e);
    }
  }

  @Override
  public List<Object> fetchAttachments(final int execId, final String jobId, final int attempt)
      throws ExecutorManagerException {

    return this.executionJobDao.fetchAttachments(execId, jobId, attempt);
  }

  @Override
  public void uploadLogFile(final int execId, final String name, final int attempt, final File... files)
      throws ExecutorManagerException {
    final Connection connection = getConnection();
    try {
      uploadLogFile(connection, execId, name, attempt, files,
          this.defaultEncodingType);
      connection.commit();
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error committing log", e);
    } catch (final IOException e) {
      throw new ExecutorManagerException("Error committing log", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void uploadLogFile(final Connection connection, final int execId, final String name,
                             final int attempt, final File[] files, final EncodingType encType)
      throws ExecutorManagerException, IOException {
    // 50K buffer... if logs are greater than this, we chunk.
    // However, we better prevent large log files from being uploaded somehow
    final byte[] buffer = new byte[50 * 1024];
    int pos = 0;
    int length = buffer.length;
    int startByte = 0;
    try {
      for (int i = 0; i < files.length; ++i) {
        final File file = files[i];

        final BufferedInputStream bufferedStream =
            new BufferedInputStream(new FileInputStream(file));
        try {
          int size = bufferedStream.read(buffer, pos, length);
          while (size >= 0) {
            if (pos + size == buffer.length) {
              // Flush here.
              uploadLogPart(connection, execId, name, attempt, startByte,
                  startByte + buffer.length, encType, buffer, buffer.length);

              pos = 0;
              length = buffer.length;
              startByte += buffer.length;
            } else {
              // Usually end of file.
              pos += size;
              length = buffer.length - pos;
            }
            size = bufferedStream.read(buffer, pos, length);
          }
        } finally {
          IOUtils.closeQuietly(bufferedStream);
        }
      }

      // Final commit of buffer.
      if (pos > 0) {
        uploadLogPart(connection, execId, name, attempt, startByte, startByte
            + pos, encType, buffer, pos);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error writing log part.", e);
    } catch (final IOException e) {
      throw new ExecutorManagerException("Error chunking", e);
    }
  }

  private void uploadLogPart(final Connection connection, final int execId, final String name,
                             final int attempt, final int startByte, final int endByte, final EncodingType encType,
                             final byte[] buffer, final int length) throws SQLException, IOException {
    final String INSERT_EXECUTION_LOGS =
        "INSERT INTO execution_logs "
            + "(exec_id, name, attempt, enc_type, start_byte, end_byte, "
            + "log, upload_time) VALUES (?,?,?,?,?,?,?,?)";

    final QueryRunner runner = new QueryRunner();
    byte[] buf = buffer;
    if (encType == EncodingType.GZIP) {
      buf = GZIPUtils.gzipBytes(buf, 0, length);
    } else if (length < buf.length) {
      buf = Arrays.copyOf(buffer, length);
    }

    runner.update(connection, INSERT_EXECUTION_LOGS, execId, name, attempt,
        encType.getNumVal(), startByte, startByte + length, buf, DateTime.now()
            .getMillis());
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
    final String UPDATE =
      "UPDATE executors SET host=?, port=?, active=? where id=?";

    final QueryRunner runner = createQueryRunner();
    try {
      final int rows =
        runner.update(UPDATE, executor.getHost(), executor.getPort(),
          executor.isActive(), executor.getId());
      if (rows == 0) {
        throw new ExecutorManagerException("No executor with id :"
          + executor.getId());
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error inactivating executor "
        + executor.getId(), e);
    }
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
    final QueryRunner runner = createQueryRunner();

    final String INSERT_PROJECT_EVENTS =
      "INSERT INTO executor_events (executor_id, event_type, event_time, username, message) values (?,?,?,?,?)";
    final Date updateDate = new Date();
    try {
      runner.update(INSERT_PROJECT_EVENTS, executor.getId(), type.getNumVal(),
        updateDate, user, message);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Failed to post executor event", e);
    }
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
    final QueryRunner runner = createQueryRunner();

    final ExecutorLogsResultHandler logHandler = new ExecutorLogsResultHandler();
    List<ExecutorLogEvent> events = null;
    try {
      events =
        runner.query(ExecutorLogsResultHandler.SELECT_EXECUTOR_EVENTS_ORDER,
          logHandler, executor.getId(), num, offset);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
        "Failed to fetch events for executor id : " + executor.getId(), e);
    }

    return events;
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
    final String DELETE_BY_TIME =
        "DELETE FROM execution_logs WHERE upload_time < ?";

    final QueryRunner runner = createQueryRunner();
    int updateNum = 0;
    try {
      updateNum = runner.update(DELETE_BY_TIME, millis);
    } catch (final SQLException e) {
      e.printStackTrace();
      throw new ExecutorManagerException(
          "Error deleting old execution_logs before " + millis, e);
    }

    return updateNum;
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

  private static class FetchLogsHandler implements ResultSetHandler<LogData> {
    private static final String FETCH_LOGS =
        "SELECT exec_id, name, attempt, enc_type, start_byte, end_byte, log "
            + "FROM execution_logs "
            + "WHERE exec_id=? AND name=? AND attempt=? AND end_byte > ? "
            + "AND start_byte <= ? ORDER BY start_byte";

    private final int startByte;
    private final int endByte;

    public FetchLogsHandler(final int startByte, final int endByte) {
      this.startByte = startByte;
      this.endByte = endByte;
    }

    @Override
    public LogData handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }

      final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      do {
        // int execId = rs.getInt(1);
        // String name = rs.getString(2);
        final int attempt = rs.getInt(3);
        final EncodingType encType = EncodingType.fromInteger(rs.getInt(4));
        final int startByte = rs.getInt(5);
        final int endByte = rs.getInt(6);

        final byte[] data = rs.getBytes(7);

        final int offset =
            this.startByte > startByte ? this.startByte - startByte : 0;
        final int length =
            this.endByte < endByte ? this.endByte - startByte - offset
                : endByte - startByte - offset;
        try {
          byte[] buffer = data;
          if (encType == EncodingType.GZIP) {
            buffer = GZIPUtils.unGzipBytes(data);
          }

          byteStream.write(buffer, offset, length);
        } catch (final IOException e) {
          throw new SQLException(e);
        }
      } while (rs.next());

      final byte[] buffer = byteStream.toByteArray();
      final Pair<Integer, Integer> result =
          FileIOUtils.getUtf8Range(buffer, 0, buffer.length);

      return new LogData(this.startByte + result.getFirst(), result.getSecond(),
          new String(buffer, result.getFirst(), result.getSecond(), StandardCharsets.UTF_8));
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

  private static class FetchActiveExecutableFlows implements
      ResultSetHandler<Map<Integer, Pair<ExecutionReference, ExecutableFlow>>> {
    // Select running and executor assigned flows
    private static final String FETCH_ACTIVE_EXECUTABLE_FLOW =
      "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
        + "et.port port, et.id executorId, et.active executorStatus"
        + " FROM execution_flows ex"
        + " INNER JOIN "
        + " executors et ON ex.executor_id = et.id"
        + " Where ex.status NOT IN ("
        + Status.SUCCEEDED.getNumVal() + ", "
        + Status.KILLED.getNumVal() + ", "
        + Status.FAILED.getNumVal() + ")";

    @Override
    public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> handle(
        final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyMap();
      }

      final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> execFlows =
          new HashMap<>();
      do {
        final int id = rs.getInt(1);
        final int encodingType = rs.getInt(2);
        final byte[] data = rs.getBytes(3);
        final String host = rs.getString(4);
        final int port = rs.getInt(5);
        final int executorId = rs.getInt(6);
        final boolean executorStatus = rs.getBoolean(7);

        if (data == null) {
          execFlows.put(id, null);
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
            final Executor executor = new Executor(executorId, host, port, executorStatus);
            final ExecutionReference ref = new ExecutionReference(id, executor);
            execFlows.put(id, new Pair<>(ref, exFlow));
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }

  private static class FetchActiveExecutableFlowByExecId implements
      ResultSetHandler<List<Pair<ExecutionReference, ExecutableFlow>>> {
    private static final String FETCH_ACTIVE_EXECUTABLE_FLOW_BY_EXECID =
        "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
            + "et.port port, et.id executorId, et.active executorStatus"
            + " FROM execution_flows ex"
            + " INNER JOIN "
            + " executors et ON ex.executor_id = et.id"
            + " Where ex.exec_id = ? AND ex.status NOT IN ("
            + Status.SUCCEEDED.getNumVal() + ", "
            + Status.KILLED.getNumVal() + ", "
            + Status.FAILED.getNumVal() + ")";

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
        final String host = rs.getString(4);
        final int port = rs.getInt(5);
        final int executorId = rs.getInt(6);
        final boolean executorStatus = rs.getBoolean(7);

        if (data == null) {
          logger.error("Found a flow with empty data blob exec_id: " + id);
        } else {
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
            final Executor executor = new Executor(executorId, host, port, executorStatus);
            final ExecutionReference ref = new ExecutionReference(id, executor);
            execFlows.add(new Pair<>(ref, exFlow));
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

 /**
   * JDBC ResultSetHandler to fetch records from executor_events table
   */
  private static class ExecutorLogsResultHandler implements
    ResultSetHandler<List<ExecutorLogEvent>> {
    private static final String SELECT_EXECUTOR_EVENTS_ORDER =
      "SELECT executor_id, event_type, event_time, username, message FROM executor_events "
        + " WHERE executor_id=? ORDER BY event_time LIMIT ? OFFSET ?";

    @Override
    public List<ExecutorLogEvent> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutorLogEvent> emptyList();
      }

      final ArrayList<ExecutorLogEvent> events = new ArrayList<>();
      do {
        final int executorId = rs.getInt(1);
        final int eventType = rs.getInt(2);
        final Date eventTime = rs.getDate(3);
        final String username = rs.getString(4);
        final String message = rs.getString(5);

        final ExecutorLogEvent event =
          new ExecutorLogEvent(executorId, username, eventTime,
            EventType.fromInteger(eventType), message);
        events.add(event);
      } while (rs.next());

      return events;
    }
  }
}

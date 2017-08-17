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
import azkaban.utils.PropsUtils;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

@Singleton
public class JdbcExecutorLoader extends AbstractJdbcLoader implements
    ExecutorLoader {
  private static final Logger logger = Logger
      .getLogger(JdbcExecutorLoader.class);
  private final ExecutionFlowDBManager executionFlowDBManager;
  private final ExecutorDBManager executorDBManager;
  private EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  public JdbcExecutorLoader(final Props props, final CommonMetrics commonMetrics,
                            final ExecutionFlowDBManager executionFlowDBManager,
                            final ExecutorDBManager executorDBManager) {
    super(props, commonMetrics);
    this.executionFlowDBManager = executionFlowDBManager;
    this.executorDBManager = executorDBManager;
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
    this.executionFlowDBManager.uploadExecutableFlow(flow);
  }

  @Override
  public void updateExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    this.executionFlowDBManager.updateExecutableFlow(flow);
  }

  @Override
  public ExecutableFlow fetchExecutableFlow(final int id)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchExecutableFlows flowHandler = new FetchExecutableFlows();

    try {
      final List<ExecutableFlow> properties =
          runner.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW, flowHandler,
              id);
      if (properties.isEmpty()) {
        return null;
      } else {
        return properties.get(0);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching flow id " + id, e);
    }
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
    return this.executionFlowDBManager.fetchFlowHistory(projectId, flowId, skip, num);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
                                               final int skip, final int num, final Status status) throws ExecutorManagerException {
    return this.executionFlowDBManager.fetchFlowHistory(projectId, flowId, skip, num, status);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int skip, final int num)
      throws ExecutorManagerException {
    return this.executionFlowDBManager.fetchFlowHistory(skip,num);
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final String projContain,
                                               final String flowContains, final String userNameContains, final int status, final long startTime,
                                               final long endTime, final int skip, final int num) throws ExecutorManagerException {
    return this.executionFlowDBManager.fetchFlowHistory(projContain, flowContains,
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
    final String INSERT_EXECUTION_NODE =
        "INSERT INTO execution_jobs "
            + "(exec_id, project_id, version, flow_id, job_id, start_time, "
            + "end_time, status, input_params, attempt) VALUES (?,?,?,?,?,?,?,?,?,?)";

    byte[] inputParam = null;
    if (inputProps != null) {
      try {
        final String jsonString =
            JSONUtils.toJSON(PropsUtils.toHierarchicalMap(inputProps));
        inputParam = GZIPUtils.gzipString(jsonString, "UTF-8");
      } catch (final IOException e) {
        throw new ExecutorManagerException("Error encoding input params");
      }
    }

    final ExecutableFlow flow = node.getExecutableFlow();
    final String flowId = node.getParentFlow().getFlowPath();
    System.out.println("Uploading flowId " + flowId);
    final QueryRunner runner = createQueryRunner();
    try {
      runner.update(INSERT_EXECUTION_NODE, flow.getExecutionId(),
          flow.getProjectId(), flow.getVersion(), flowId, node.getId(),
          node.getStartTime(), node.getEndTime(), node.getStatus().getNumVal(),
          inputParam, node.getAttempt());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error writing job " + node.getId(), e);
    }
  }

  @Override
  public void updateExecutableNode(final ExecutableNode node)
      throws ExecutorManagerException {
    final String UPSERT_EXECUTION_NODE =
        "UPDATE execution_jobs "
            + "SET start_time=?, end_time=?, status=?, output_params=? "
            + "WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";

    byte[] outputParam = null;
    final Props outputProps = node.getOutputProps();
    if (outputProps != null) {
      try {
        final String jsonString =
            JSONUtils.toJSON(PropsUtils.toHierarchicalMap(outputProps));
        outputParam = GZIPUtils.gzipString(jsonString, "UTF-8");
      } catch (final IOException e) {
        throw new ExecutorManagerException("Error encoding input params");
      }
    }

    final QueryRunner runner = createQueryRunner();
    try {
      runner.update(UPSERT_EXECUTION_NODE, node.getStartTime(), node
          .getEndTime(), node.getStatus().getNumVal(), outputParam, node
          .getExecutableFlow().getExecutionId(), node.getParentFlow()
          .getFlowPath(), node.getId(), node.getAttempt());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating job " + node.getId(),
          e);
    }
  }

  @Override
  public List<ExecutableJobInfo> fetchJobInfoAttempts(final int execId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    try {
      final List<ExecutableJobInfo> info =
          runner.query(
              FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE_ATTEMPTS,
              new FetchExecutableJobHandler(), execId, jobId);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  @Override
  public ExecutableJobInfo fetchJobInfo(final int execId, final String jobId, final int attempts)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    try {
      final List<ExecutableJobInfo> info =
          runner.query(FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE,
              new FetchExecutableJobHandler(), execId, jobId, attempts);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info.get(0);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  @Override
  public Props fetchExecutionJobInputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    try {
      final Pair<Props, Props> props =
          runner.query(
              FetchExecutableJobPropsHandler.FETCH_INPUT_PARAM_EXECUTABLE_NODE,
              new FetchExecutableJobPropsHandler(), execId, jobId);
      return props.getFirst();
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  @Override
  public Props fetchExecutionJobOutputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    try {
      final Pair<Props, Props> props =
          runner
              .query(
                  FetchExecutableJobPropsHandler.FETCH_OUTPUT_PARAM_EXECUTABLE_NODE,
                  new FetchExecutableJobPropsHandler(), execId, jobId);
      return props.getFirst();
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  @Override
  public Pair<Props, Props> fetchExecutionJobProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    try {
      final Pair<Props, Props> props =
          runner
              .query(
                  FetchExecutableJobPropsHandler.FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE,
                  new FetchExecutableJobPropsHandler(), execId, jobId);
      return props;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  @Override
  public List<ExecutableJobInfo> fetchJobHistory(final int projectId, final String jobId,
                                                 final int skip, final int size) throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    try {
      final List<ExecutableJobInfo> info =
          runner.query(FetchExecutableJobHandler.FETCH_PROJECT_EXECUTABLE_NODE,
              new FetchExecutableJobHandler(), projectId, jobId, skip, size);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
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
    final QueryRunner runner = createQueryRunner();

    try {
      final String attachments =
          runner
              .query(
                  FetchExecutableJobAttachmentsHandler.FETCH_ATTACHMENTS_EXECUTABLE_NODE,
                  new FetchExecutableJobAttachmentsHandler(), execId, jobId);
      if (attachments == null) {
        return null;
      }

      final List<Object> attachmentList =
          (List<Object>) JSONUtils.parseJSONFromString(attachments);

      return attachmentList;
    } catch (final IOException e) {
      throw new ExecutorManagerException(
          "Error converting job attachments to JSON " + jobId, e);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error query job attachments " + jobId, e);
    }
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
    final Connection connection = getConnection();
    try {
      uploadAttachmentFile(connection, node, file, this.defaultEncodingType);
      connection.commit();
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error committing attachments ", e);
    } catch (final IOException e) {
      throw new ExecutorManagerException("Error uploading attachments ", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void uploadAttachmentFile(final Connection connection, final ExecutableNode node,
                                    final File file, final EncodingType encType) throws SQLException, IOException {

    final String jsonString = FileUtils.readFileToString(file);
    final byte[] attachments = GZIPUtils.gzipString(jsonString, "UTF-8");

    final String UPDATE_EXECUTION_NODE_ATTACHMENTS =
        "UPDATE execution_jobs " + "SET attachments=? "
            + "WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";

    final QueryRunner runner = new QueryRunner();
    runner.update(connection, UPDATE_EXECUTION_NODE_ATTACHMENTS, attachments,
        node.getExecutableFlow().getExecutionId(), node.getParentFlow()
            .getNestedId(), node.getId(), node.getAttempt());
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
    return this.executorDBManager.fetchAllExecutors();
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchActiveExecutors()
   */
  @Override
  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    return this.executorDBManager.fetchActiveExecutors();
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchExecutor(java.lang.String, int)
   */
  @Override
  public Executor fetchExecutor(final String host, final int port)
    throws ExecutorManagerException {
    return this.executorDBManager.fetchExecutor(host, port);
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchExecutor(int)
   */
  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    return this.executorDBManager.fetchExecutor(executorId);
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#updateExecutor(int)
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
    return this.executorDBManager.addExecutor(host, port);
  }


  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#removeExecutor(String, int)
   */
  @Override
  public void removeExecutor(final String host, final int port) throws ExecutorManagerException {
    this.executorDBManager.removeExecutor(host, port);
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
    return this.executorDBManager.fetchExecutorByExecutionId(executionId);
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

  private static class FetchExecutableJobHandler implements
      ResultSetHandler<List<ExecutableJobInfo>> {
    private static final String FETCH_EXECUTABLE_NODE =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt "
            + "FROM execution_jobs WHERE exec_id=? "
            + "AND job_id=? AND attempt=?";
    private static final String FETCH_EXECUTABLE_NODE_ATTEMPTS =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt FROM execution_jobs "
            + "WHERE exec_id=? AND job_id=?";
    private static final String FETCH_PROJECT_EXECUTABLE_NODE =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt FROM execution_jobs "
            + "WHERE project_id=? AND job_id=? "
            + "ORDER BY exec_id DESC LIMIT ?, ? ";

    @Override
    public List<ExecutableJobInfo> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutableJobInfo> emptyList();
      }

      final List<ExecutableJobInfo> execNodes = new ArrayList<>();
      do {
        final int execId = rs.getInt(1);
        final int projectId = rs.getInt(2);
        final int version = rs.getInt(3);
        final String flowId = rs.getString(4);
        final String jobId = rs.getString(5);
        final long startTime = rs.getLong(6);
        final long endTime = rs.getLong(7);
        final Status status = Status.fromInteger(rs.getInt(8));
        final int attempt = rs.getInt(9);

        final ExecutableJobInfo info =
            new ExecutableJobInfo(execId, projectId, version, flowId, jobId,
                startTime, endTime, status, attempt);
        execNodes.add(info);
      } while (rs.next());

      return execNodes;
    }
  }

  private static class FetchExecutableJobAttachmentsHandler implements
      ResultSetHandler<String> {
    private static final String FETCH_ATTACHMENTS_EXECUTABLE_NODE =
        "SELECT attachments FROM execution_jobs WHERE exec_id=? AND job_id=?";

    @Override
    public String handle(final ResultSet rs) throws SQLException {
      String attachmentsJson = null;
      if (rs.next()) {
        try {
          final byte[] attachments = rs.getBytes(1);
          if (attachments != null) {
            attachmentsJson = GZIPUtils.unGzipString(attachments, "UTF-8");
          }
        } catch (final IOException e) {
          throw new SQLException("Error decoding job attachments", e);
        }
      }
      return attachmentsJson;
    }
  }

  private static class FetchExecutableJobPropsHandler implements
      ResultSetHandler<Pair<Props, Props>> {
    private static final String FETCH_OUTPUT_PARAM_EXECUTABLE_NODE =
        "SELECT output_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
    private static final String FETCH_INPUT_PARAM_EXECUTABLE_NODE =
        "SELECT input_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
    private static final String FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE =
        "SELECT input_params, output_params "
            + "FROM execution_jobs WHERE exec_id=? AND job_id=?";

    @Override
    public Pair<Props, Props> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return new Pair<>(null, null);
      }

      if (rs.getMetaData().getColumnCount() > 1) {
        final byte[] input = rs.getBytes(1);
        final byte[] output = rs.getBytes(2);

        Props inputProps = null;
        Props outputProps = null;
        try {
          if (input != null) {
            final String jsonInputString = GZIPUtils.unGzipString(input, "UTF-8");
            inputProps =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonInputString));

          }
          if (output != null) {
            final String jsonOutputString = GZIPUtils.unGzipString(output, "UTF-8");
            outputProps =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonOutputString));
          }
        } catch (final IOException e) {
          throw new SQLException("Error decoding param data", e);
        }

        return new Pair<>(inputProps, outputProps);
      } else {
        final byte[] params = rs.getBytes(1);
        Props props = null;
        try {
          if (params != null) {
            final String jsonProps = GZIPUtils.unGzipString(params, "UTF-8");

            props =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonProps));
          }
        } catch (final IOException e) {
          throw new SQLException("Error decoding param data", e);
        }

        return new Pair<>(props, null);
      }
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

  private static class FetchExecutableFlows implements
      ResultSetHandler<List<ExecutableFlow>> {
    private static final String FETCH_BASE_EXECUTABLE_FLOW_QUERY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows ";
    private static final String FETCH_EXECUTABLE_FLOW =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE exec_id=?";
    // private static String FETCH_ACTIVE_EXECUTABLE_FLOW =
    // "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data "
    // +
    // "FROM execution_flows ex " +
    // "INNER JOIN active_executing_flows ax ON ex.exec_id = ax.exec_id";
    private static final String FETCH_ALL_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    private static final String FETCH_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    private static final String FETCH_EXECUTABLE_FLOW_BY_STATUS =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? AND status=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";

    @Override
    public List<ExecutableFlow> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutableFlow> emptyList();
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
            // Convoluted way to inflate strings. Should find common package
            // or helper function.
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

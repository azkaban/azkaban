package azkaban.executor;

import azkaban.database.AbstractJdbcLoader;
import azkaban.database.EncodingType;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseTransOperator;
import azkaban.db.SQLTransaction;
import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.executor.JdbcExecutorHandlerSet.ExecutorLogsResultHandler;
import azkaban.executor.JdbcExecutorHandlerSet.FetchActiveExecutableFlowByExecId;
import azkaban.executor.JdbcExecutorHandlerSet.FetchActiveExecutableFlows;
import azkaban.executor.JdbcExecutorHandlerSet.FetchExecutableFlows;
import azkaban.executor.JdbcExecutorHandlerSet.FetchExecutableJobAttachmentsHandler;
import azkaban.executor.JdbcExecutorHandlerSet.FetchExecutableJobHandler;
import azkaban.executor.JdbcExecutorHandlerSet.FetchExecutableJobPropsHandler;
import azkaban.executor.JdbcExecutorHandlerSet.FetchExecutorHandler;
import azkaban.executor.JdbcExecutorHandlerSet.FetchLogsHandler;
import azkaban.executor.JdbcExecutorHandlerSet.FetchQueuedExecutableFlows;
import azkaban.executor.JdbcExecutorHandlerSet.FetchRecentlyFinishedFlows;
import azkaban.executor.JdbcExecutorHandlerSet.IntHandler;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public class JdbcExecutorImpl implements ExecutorLoader {

  private static final Logger logger = Logger.getLogger(JdbcExecutorImpl.class);
  private final DatabaseOperator dbOperator;

  public JdbcExecutorImpl(final DatabaseOperator databaseOperator) {
    this.dbOperator = databaseOperator;
  }

  @Override
  public synchronized void uploadExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {
    final String INSERT_EXECUTABLE_FLOW = "INSERT INTO execution_flows "
        + "(project_id, flow_id, version, status, submit_time, submit_user, update_time) "
        + "values (?,?,?,?,?,?,?)";
    final long submitTime = System.currentTimeMillis();

    final SQLTransaction<Long> insertAndGetLastID = transOperator -> {
      transOperator.update(INSERT_EXECUTABLE_FLOW, flow.getProjectId(),
          flow.getFlowId(), flow.getVersion(), Status.PREPARING.getNumVal(),
          submitTime, flow.getSubmitUser(), submitTime);
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };

    try {
      final long id = this.dbOperator.transaction(insertAndGetLastID);
      logger.info("Flow given " + flow.getFlowId() + " given id " + id);
      flow.setExecutionId((int) id);
      updateExecutableFlow(flow);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error creating execution.", e);
    }
  }

  @Override
  public ExecutableFlow fetchExecutableFlow(final int execId) throws ExecutorManagerException {
    final FetchExecutableFlows flowHandler = new FetchExecutableFlows();
    try {
      final List<ExecutableFlow> properties = this.dbOperator
          .query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW,
              flowHandler, id);
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
   * maxAge indicates how long finished flows are shown in Recently Finished flow page.
   */
  @Override
  public List<ExecutableFlow> fetchRecentlyFinishedFlows(final Duration maxAge)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchRecentlyFinishedFlows.FETCH_RECENTLY_FINISHED_FLOW,
          new FetchRecentlyFinishedFlows(), System.currentTimeMillis() - maxAge.toMillis(),
          Status.SUCCEEDED.getNumVal(), Status.KILLED.getNumVal(),
          Status.FAILED.getNumVal());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching recently finished flows", e);
    }
  }

  @Override
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchActiveExecutableFlows.FETCH_ACTIVE_EXECUTABLE_FLOW,
          new FetchActiveExecutableFlows());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(final int execId)
      throws ExecutorManagerException {
    try {
      final List<Pair<ExecutionReference, ExecutableFlow>> flows =
          this.dbOperator
              .query(FetchActiveExecutableFlowByExecId.FETCH_ACTIVE_EXECUTABLE_FLOW_BY_EXECID,
                  new FetchActiveExecutableFlowByExecId(), execId);
      if (flows.isEmpty()) {
        return null;
      } else {
        return flows.get(0);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows by exec id", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int skip, final int num)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchExecutableFlows.FETCH_ALL_EXECUTABLE_FLOW_HISTORY,
          new FetchExecutableFlows(), skip, num);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching flow History", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
                                               final int skip, final int num)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_HISTORY,
          new FetchExecutableFlows(), projectId, flowId, skip, num);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching flow history", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
                                               final int skip, final int num,
                                               final Status status)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_BY_STATUS,
          new FetchExecutableFlows(), projectId, flowId, status.getNumVal(), skip, num);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(final String projContain, final String flowContains,
                                               final String userNameContains, final int status,
                                               final long startData,
                                               final long endData, final int skip, final int num)
      throws ExecutorManagerException {
    String query = FetchExecutableFlows.FETCH_BASE_EXECUTABLE_FLOW_QUERY;
    final List<Object> params = new ArrayList<Object>();

    boolean first = true;
    if (projContain != null && !projContain.isEmpty()) {
      query += " ef JOIN projects p ON ef.project_id = p.id WHERE name LIKE ?";
      params.add('%' + projContain + '%');
      first = false;
    }

    // todo kunkun-tang: we don't need the below complicated logics. We should just use a simple way.
    if (flowContains != null && !flowContains.isEmpty()) {
      if (first) {
        query += " WHERE ";
        first = false;
      } else {
        query += " AND ";
      }

      query += " flow_id LIKE ?";
      params.add('%' + flowContains + '%');
    }

    if (userNameContains != null && !userNameContains.isEmpty()) {
      if (first) {
        query += " WHERE ";
        first = false;
      } else {
        query += " AND ";
      }
      query += " submit_user LIKE ?";
      params.add('%' + userNameContains + '%');
    }

    if (status != 0) {
      if (first) {
        query += " WHERE ";
        first = false;
      } else {
        query += " AND ";
      }
      query += " status = ?";
      params.add(status);
    }

    if (startTime > 0) {
      if (first) {
        query += " WHERE ";
        first = false;
      } else {
        query += " AND ";
      }
      query += " start_time > ?";
      params.add(startTime);
    }

    if (endTime > 0) {
      if (first) {
        query += " WHERE ";
        first = false;
      } else {
        query += " AND ";
      }
      query += " end_time < ?";
      params.add(endTime);
    }

    if (skip > -1 && num > 0) {
      query += "  ORDER BY exec_id DESC LIMIT ?, ?";
      params.add(skip);
      params.add(num);
    }

    try {
      return this.dbOperator.query(query, new FetchExecutableFlows(), params.toArray());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public List<Executor> fetchAllExecutors() throws ExecutorManagerException {
    try {
      return this.dbOperator
          .query(FetchExecutableFlows.FETCH_ALL_EXECUTORS, new FetchExecutorHandler());
    } catch (final Exception e) {
      throw new ExecutorManagerException("Error fetching executors", e);
    }
  }

  @Override
  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    try {
      return this.dbOperator
          .query(FetchExecutorHandler.FETCH_ACTIVE_EXECUTORS, new FetchExecutorHandler());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active executors", e);
    }
  }

  @Override
  public Executor fetchExecutor(final String host, final int port) throws ExecutorManagerException {
    try {
      final List<Executor> executors =
          this.dbOperator.query(FetchExecutorHandler.FETCH_EXECUTOR_BY_HOST_PORT,
              new FetchExecutorHandler(), host, port);
      if (executors.isEmpty()) {
        return null;
      } else {
        return executors.get(0);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(String.format(
          "Error fetching executor %s:%d", host, port), e);
    }
  }

  @Override
  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    try {
      final List<Executor> executors = this.dbOperator
          .query(FetchExecutorHandler.FETCH_EXECUTOR_BY_ID,
              new FetchExecutorHandler(), executorId);
      if (executors.isEmpty()) {
        return null;
      } else {
        return executors.get(0);
      }
    } catch (final Exception e) {
      throw new ExecutorManagerException(String.format(
          "Error fetching executor with id: %d", executorId), e);
    }
  }

  @Override
  public Executor addExecutor(final String host, final int port) throws ExecutorManagerException {
    // verify, if executor already exists
    if (fetchExecutor(host, port) != null) {
      throw new ExecutorManagerException(String.format(
          "Executor %s:%d already exist", host, port));
    }
    // add new executor
    addExecutorHelper(host, port);

    // fetch newly added executor
    return fetchExecutor(host, port);
  }

  private void addExecutorHelper(final String host, final int port)
      throws ExecutorManagerException {
    final String INSERT = "INSERT INTO executors (host, port) values (?,?)";
    try {
      this.dbOperator.update(INSERT, host, port);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(String.format("Error adding %s:%d ",
          host, port), e);
    }
  }

  @Override
  public void updateExecutor(final Executor executor) throws ExecutorManagerException {
    final String UPDATE =
        "UPDATE executors SET host=?, port=?, active=? where id=?";

    try {
      final int rows = this.dbOperator.update(UPDATE, executor.getHost(), executor.getPort(),
          executor.isActive(), executor.getId());
      if (rows == 0) {
        throw new ExecutorManagerException("No executor with id :" + executor.getId());
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error inactivating executor "
          + executor.getId(), e);
    }
  }

  @Override
  public void removeExecutor(final String host, final int port) throws ExecutorManagerException {
    final String DELETE = "DELETE FROM executors WHERE host=? AND port=?";
    try {
      final int rows = this.dbOperator.update(DELETE, host, port);
      if (rows == 0) {
        throw new ExecutorManagerException("No executor with host, port :"
            + "(" + host + "," + port + ")");
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error removing executor with host, port : "
          + "(" + host + "," + port + ")", e);
    }
  }

  @Override
  public void postExecutorEvent(final Executor executor, final EventType type, final String user,
                                final String message)
      throws ExecutorManagerException {
    final String INSERT_PROJECT_EVENTS =
        "INSERT INTO executor_events (executor_id, event_type, event_time, username, message) values (?,?,?,?,?)";
    try {
      this.dbOperator.update(INSERT_PROJECT_EVENTS, executor.getId(), type.getNumVal(),
          new Date(), user, message);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Failed to post executor event", e);
    }
  }

  @Override
  public List<ExecutorLogEvent> getExecutorEvents(final Executor executor, final int num,
                                                  final int offset)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(ExecutorLogsResultHandler.SELECT_EXECUTOR_EVENTS_ORDER,
          new ExecutorLogsResultHandler(), executor.getId(), num, offset);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Failed to fetch events for executor id : " + executor.getId(), e);
    }
  }

  @Override
  public void addActiveExecutableReference(final ExecutionReference reference)
      throws ExecutorManagerException {
    final String INSERT = "INSERT INTO active_executing_flows "
        + "(exec_id, update_time) values (?,?)";
    try {
      this.dbOperator.update(INSERT, reference.getExecId(), reference.getUpdateTime());
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error updating active flow reference " + reference.getExecId(), e);
    }
  }

  @Override
  public void removeActiveExecutableReference(final int execId) throws ExecutorManagerException {
    final String DELETE = "DELETE FROM active_executing_flows WHERE exec_id=?";
    try {
      this.dbOperator.update(DELETE, execId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execId, e);
    }
  }

  @Override
  public void unassignExecutor(final int executionId) throws ExecutorManagerException {
    final String UPDATE = "UPDATE execution_flows SET executor_id=NULL where exec_id=?";
    try {
      final int rows = this.dbOperator.update(UPDATE, executionId);
      if (rows == 0) {
        throw new ExecutorManagerException(String.format(
            "Failed to unassign executor for execution : %d  ", executionId));
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating execution id " + executionId, e);
    }

  }

  @Override
  public void assignExecutor(final int executorId, final int executionId)
      throws ExecutorManagerException {
    final String UPDATE =
        "UPDATE execution_flows SET executor_id=? where exec_id=?";
    try {
      if (fetchExecutor(executorId) == null) {
        throw new ExecutorManagerException(String.format(
            "Failed to assign non-existent executor Id: %d to execution : %d  ",
            executorId, executionId));
      }

      if (this.dbOperator.update(UPDATE, executorId, executionId) == 0) {
        throw new ExecutorManagerException(String.format(
            "Failed to assign executor Id: %d to non-existent execution : %d  ",
            executorId, executionId));
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating executor id "
          + executorId, e);
    }
  }

  @Override
  public Executor fetchExecutorByExecutionId(final int executionId)
      throws ExecutorManagerException {
    final FetchExecutorHandler executorHandler = new FetchExecutorHandler();
    try {
      final List<Executor> executors = this.dbOperator
          .query(FetchExecutorHandler.FETCH_EXECUTION_EXECUTOR,
              executorHandler, executionId);
      if (executors.size() > 0) {
        return executors.get(0);
      } else {
        return null;
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error fetching executor for exec_id : " + executionId, e);
    }
  }

  @Override
  public List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchQueuedExecutableFlows.FETCH_QUEUED_EXECUTABLE_FLOW,
          new FetchQueuedExecutableFlows());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public boolean updateExecutableReference(final int execId, final long updateTime)
      throws ExecutorManagerException {
    final String DELETE =
        "UPDATE active_executing_flows set update_time=? WHERE exec_id=?";

    int updateNum = 0;
    try {
      updateNum = this.dbOperator.update(DELETE, updateTime, execId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execId, e);
    }

    // Should be 1.
    return updateNum > 0;
  }

  // TODO kunkun-tang: the interface's parameter is called endByte, but actually is length.
  @Override
  public LogData fetchLogs(final int execId, final String name, final int attempt,
                           final int startByte, final int length)
      throws ExecutorManagerException {
    final FetchLogsHandler handler = new FetchLogsHandler(startByte, length + startByte);
    try {
      return this.dbOperator.query(FetchLogsHandler.FETCH_LOGS, handler,
          execId, name, attempt, startByte, startByte + length);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching logs " + execId
          + " : " + name, e);
    }
  }

  // TODO kunkun-tang: the interface's parameter is called name, but actually is jobId.
  @Override
  public List<Object> fetchAttachments(final int execId, final String jobId, final int attempt)
      throws ExecutorManagerException {
    try {
      final String attachments = this.dbOperator.query(
          FetchExecutableJobAttachmentsHandler.FETCH_ATTACHMENTS_EXECUTABLE_NODE,
          new FetchExecutableJobAttachmentsHandler(), execId, jobId);
      if (attachments == null) {
        return null;
      } else {
        return (List<Object>) JSONUtils.parseJSONFromString(attachments);
      }
    } catch (final IOException e) {
      throw new ExecutorManagerException(
          "Error converting job attachments to JSON " + jobId, e);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error query job attachments " + jobId, e);
    }
  }

  @Override
  public void uploadLogFile(final int execId, final String name, final int attempt,
                            final File... files)
      throws ExecutorManagerException {
    final SQLTransaction<Integer> transaction = transOperator -> {
      addProjectToProjectVersions(transOperator, projectId, version, localFile, uploader, md5,
          resourceId);
      return 1;
    };
  }

  private void uploadLogPart(final DatabaseTransOperator transOperator, final int execId,
                             final String name,
                             final int attempt, final int startByte, final int endByte,
                             final AbstractJdbcLoader.EncodingType encType,
                             final byte[] buffer, final int length)
      throws SQLException, IOException {
    final String INSERT_EXECUTION_LOGS = "INSERT INTO execution_logs "
        + "(exec_id, name, attempt, enc_type, start_byte, end_byte, "
        + "log, upload_time) VALUES (?,?,?,?,?,?,?,?)";

    byte[] buf = buffer;
    if (encType == AbstractJdbcLoader.EncodingType.GZIP) {
      buf = GZIPUtils.gzipBytes(buf, 0, length);
    } else if (length < buf.length) {
      buf = Arrays.copyOf(buffer, length);
    }

    transOperator.update(INSERT_EXECUTION_LOGS, execId, name, attempt,
        encType.getNumVal(), startByte, startByte + length, buf, DateTime.now()
            .getMillis());
  }

  @Override
  public void uploadAttachmentFile(final ExecutableNode node, final File file)
      throws ExecutorManagerException {
    final String UPDATE_EXECUTION_NODE_ATTACHMENTS =
        "UPDATE execution_jobs " + "SET attachments=? "
            + "WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";
    try {
      final String jsonString = FileUtils.readFileToString(file);
      final byte[] attachments = GZIPUtils.gzipString(jsonString, "UTF-8");
      this.dbOperator.update(UPDATE_EXECUTION_NODE_ATTACHMENTS, attachments,
          node.getExecutableFlow().getExecutionId(), node.getParentFlow()
              .getNestedId(), node.getId(), node.getAttempt());
    } catch (final IOException | SQLException e) {
      throw new ExecutorManagerException("Error uploading attachments.", e);
    }
  }

  @Override
  public void updateExecutableFlow(final ExecutableFlow flow) throws ExecutorManagerException {
    updateExecutableFlow(flow, EncodingType.GZIP);
  }

  public void updateExecutableFlow(final ExecutableFlow flow, final EncodingType encType)
      throws ExecutorManagerException {
    final String UPDATE_EXECUTABLE_FLOW_DATA =
        "UPDATE execution_flows "
            + "SET status=?,update_time=?,start_time=?,end_time=?,enc_type=?,flow_data=? "
            + "WHERE exec_id=?";

    final String json = JSONUtils.toJSON(flow.toObject());
    byte[] data = null;
    try {
      final byte[] stringData = json.getBytes("UTF-8");
      data = stringData;
      // Todo kunkun-tang: use a common method to transform stringData to data.
      if (encType == EncodingType.GZIP) {
        data = GZIPUtils.gzipBytes(stringData);
      }
    } catch (final IOException e) {
      throw new ExecutorManagerException("Error encoding the execution flow.");
    }

    try {
      this.dbOperator.update(UPDATE_EXECUTABLE_FLOW_DATA, flow.getStatus()
          .getNumVal(), flow.getUpdateTime(), flow.getStartTime(), flow
          .getEndTime(), encType.getNumVal(), data, flow.getExecutionId());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating flow.", e);
    }
  }

  @Override
  public void uploadExecutableNode(final ExecutableNode node, final Props inputProps)
      throws ExecutorManagerException {
    final String INSERT_EXECUTION_NODE = "INSERT INTO execution_jobs "
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
    logger.info("Uploading flowId " + flowId);
    try {
      this.dbOperator.update(INSERT_EXECUTION_NODE, flow.getExecutionId(),
          flow.getProjectId(), flow.getVersion(), flowId, node.getId(),
          node.getStartTime(), node.getEndTime(), node.getStatus().getNumVal(),
          inputParam, node.getAttempt());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error writing job " + node.getId(), e);
    }
  }

  @Override
  public List<ExecutableJobInfo> fetchJobInfoAttempts(final int execId, final String jobId)
      throws ExecutorManagerException {
    try {
      final List<ExecutableJobInfo> info = this.dbOperator.query(
          FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE_ATTEMPTS,
          new FetchExecutableJobHandler(), execId, jobId);
      if (info == null || info.isEmpty()) {
        return null;
      } else {
        return info;
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  @Override
  public ExecutableJobInfo fetchJobInfo(final int execId, final String jobId, final int attempts)
      throws ExecutorManagerException {
    try {
      final List<ExecutableJobInfo> info =
          this.dbOperator.query(FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE,
              new FetchExecutableJobHandler(), execId, jobId, attempts);
      if (info == null || info.isEmpty()) {
        return null;
      } else {
        return info.get(0);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  @Override
  public List<ExecutableJobInfo> fetchJobHistory(final int projectId,
                                                 final String jobId,
                                                 final int skip,
                                                 final int size) throws ExecutorManagerException {
    try {
      final List<ExecutableJobInfo> info =
          this.dbOperator.query(FetchExecutableJobHandler.FETCH_PROJECT_EXECUTABLE_NODE,
              new FetchExecutableJobHandler(), projectId, jobId, skip, size);
      if (info == null || info.isEmpty()) {
        return null;
      } else {
        return info;
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  @Override
  public void updateExecutableNode(final ExecutableNode node) throws ExecutorManagerException {
    final String UPSERT_EXECUTION_NODE = "UPDATE execution_jobs "
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
    try {
      this.dbOperator.update(UPSERT_EXECUTION_NODE, node.getStartTime(), node
          .getEndTime(), node.getStatus().getNumVal(), outputParam, node
          .getExecutableFlow().getExecutionId(), node.getParentFlow()
          .getFlowPath(), node.getId(), node.getAttempt());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating job " + node.getId(), e);
    }
  }

  @Override
  public int fetchNumExecutableFlows(final int projectId, final String flowId)
      throws ExecutorManagerException {
    final IntHandler intHandler = new IntHandler();
    try {
      return this.dbOperator.query(IntHandler.NUM_FLOW_EXECUTIONS, intHandler, projectId, flowId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public int fetchNumExecutableFlows() throws ExecutorManagerException {
    try {
      return this.dbOperator.query(IntHandler.NUM_EXECUTIONS, new IntHandler());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public int fetchNumExecutableNodes(final int projectId, final String jobId)
      throws ExecutorManagerException {
    final IntHandler intHandler = new IntHandler();
    try {
      return this.dbOperator.query(IntHandler.NUM_JOB_EXECUTIONS, intHandler, projectId, jobId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public Props fetchExecutionJobInputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    try {
      final Pair<Props, Props> props = this.dbOperator.query(
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
    try {
      final Pair<Props, Props> props = this.dbOperator.query(
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
    try {
      return this.dbOperator.query(
          FetchExecutableJobPropsHandler.FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE,
          new FetchExecutableJobPropsHandler(), execId, jobId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  @Override
  public int removeExecutionLogsByTime(final long millis) throws ExecutorManagerException {
    final String DELETE_BY_TIME =
        "DELETE FROM execution_logs WHERE upload_time < ?";
    try {
      return this.dbOperator.update(DELETE_BY_TIME, millis);
    } catch (final SQLException e) {
      e.printStackTrace();
      throw new ExecutorManagerException(
          "Error deleting old execution_logs before " + millis, e);
    }
  }
}

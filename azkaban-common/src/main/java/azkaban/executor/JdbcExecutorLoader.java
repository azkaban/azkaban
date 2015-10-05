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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.Inherited;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import azkaban.database.AbstractJdbcLoader;
import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;

public class JdbcExecutorLoader extends AbstractJdbcLoader implements
    ExecutorLoader {
  private static final Logger logger = Logger
      .getLogger(JdbcExecutorLoader.class);

  private EncodingType defaultEncodingType = EncodingType.GZIP;

  public JdbcExecutorLoader(Props props) {
    super(props);
  }

  public EncodingType getDefaultEncodingType() {
    return defaultEncodingType;
  }

  public void setDefaultEncodingType(EncodingType defaultEncodingType) {
    this.defaultEncodingType = defaultEncodingType;
  }

  @Override
  public synchronized void uploadExecutableFlow(ExecutableFlow flow)
      throws ExecutorManagerException {
    Connection connection = getConnection();
    try {
      uploadExecutableFlow(connection, flow, defaultEncodingType);
    } catch (IOException e) {
      throw new ExecutorManagerException("Error uploading flow", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private synchronized void uploadExecutableFlow(Connection connection,
      ExecutableFlow flow, EncodingType encType)
      throws ExecutorManagerException, IOException {
    final String INSERT_EXECUTABLE_FLOW =
        "INSERT INTO execution_flows "
            + "(project_id, flow_id, version, status, submit_time, submit_user, update_time) "
            + "values (?,?,?,?,?,?,?)";
    QueryRunner runner = new QueryRunner();
    long submitTime = System.currentTimeMillis();

    long id;
    try {
      flow.setStatus(Status.PREPARING);
      runner.update(connection, INSERT_EXECUTABLE_FLOW, flow.getProjectId(),
          flow.getFlowId(), flow.getVersion(), Status.PREPARING.getNumVal(),
          submitTime, flow.getSubmitUser(), submitTime);
      connection.commit();
      id =
          runner.query(connection, LastInsertID.LAST_INSERT_ID,
              new LastInsertID());

      if (id == -1L) {
        throw new ExecutorManagerException(
            "Execution id is not properly created.");
      }
      logger.info("Flow given " + flow.getFlowId() + " given id " + id);
      flow.setExecutionId((int) id);

      updateExecutableFlow(connection, flow, encType);
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error creating execution.", e);
    }
  }

  @Override
  public void updateExecutableFlow(ExecutableFlow flow)
      throws ExecutorManagerException {
    Connection connection = this.getConnection();

    try {
      updateExecutableFlow(connection, flow, defaultEncodingType);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void updateExecutableFlow(Connection connection, ExecutableFlow flow,
      EncodingType encType) throws ExecutorManagerException {
    final String UPDATE_EXECUTABLE_FLOW_DATA =
        "UPDATE execution_flows "
            + "SET status=?,update_time=?,start_time=?,end_time=?,enc_type=?,flow_data=? "
            + "WHERE exec_id=?";
    QueryRunner runner = new QueryRunner();

    String json = JSONUtils.toJSON(flow.toObject());
    byte[] data = null;
    try {
      byte[] stringData = json.getBytes("UTF-8");
      data = stringData;

      if (encType == EncodingType.GZIP) {
        data = GZIPUtils.gzipBytes(stringData);
      }
    } catch (IOException e) {
      throw new ExecutorManagerException("Error encoding the execution flow.");
    }

    try {
      runner.update(connection, UPDATE_EXECUTABLE_FLOW_DATA, flow.getStatus()
          .getNumVal(), flow.getUpdateTime(), flow.getStartTime(), flow
          .getEndTime(), encType.getNumVal(), data, flow.getExecutionId());
      connection.commit();
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error updating flow.", e);
    }
  }

  @Override
  public ExecutableFlow fetchExecutableFlow(int id)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    FetchExecutableFlows flowHandler = new FetchExecutableFlows();

    try {
      List<ExecutableFlow> properties =
          runner.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW, flowHandler,
              id);
      if (properties.isEmpty()) {
        return null;
      } else {
        return properties.get(0);
      }
    } catch (SQLException e) {
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
    QueryRunner runner = createQueryRunner();
    FetchQueuedExecutableFlows flowHandler = new FetchQueuedExecutableFlows();

    try {
      List<Pair<ExecutionReference, ExecutableFlow>> flows =
        runner.query(FetchQueuedExecutableFlows.FETCH_QUEUED_EXECUTABLE_FLOW,
          flowHandler);
      return flows;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    FetchActiveExecutableFlows flowHandler = new FetchActiveExecutableFlows();

    try {
      Map<Integer, Pair<ExecutionReference, ExecutableFlow>> properties =
          runner.query(FetchActiveExecutableFlows.FETCH_ACTIVE_EXECUTABLE_FLOW,
              flowHandler);
      return properties;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public int fetchNumExecutableFlows() throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    IntHandler intHandler = new IntHandler();
    try {
      int count = runner.query(IntHandler.NUM_EXECUTIONS, intHandler);
      return count;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public int fetchNumExecutableFlows(int projectId, String flowId)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    IntHandler intHandler = new IntHandler();
    try {
      int count =
          runner.query(IntHandler.NUM_FLOW_EXECUTIONS, intHandler, projectId,
              flowId);
      return count;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public int fetchNumExecutableNodes(int projectId, String jobId)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    IntHandler intHandler = new IntHandler();
    try {
      int count =
          runner.query(IntHandler.NUM_JOB_EXECUTIONS, intHandler, projectId,
              jobId);
      return count;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId,
      int skip, int num) throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    FetchExecutableFlows flowHandler = new FetchExecutableFlows();

    try {
      List<ExecutableFlow> properties =
          runner.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_HISTORY,
              flowHandler, projectId, flowId, skip, num);
      return properties;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId,
      int skip, int num, Status status) throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    FetchExecutableFlows flowHandler = new FetchExecutableFlows();

    try {
      List<ExecutableFlow> properties =
          runner.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_BY_STATUS,
              flowHandler, projectId, flowId, status.getNumVal(), skip, num);
      return properties;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(int skip, int num)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    FetchExecutableFlows flowHandler = new FetchExecutableFlows();

    try {
      List<ExecutableFlow> properties =
          runner.query(FetchExecutableFlows.FETCH_ALL_EXECUTABLE_FLOW_HISTORY,
              flowHandler, skip, num);
      return properties;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public List<ExecutableFlow> fetchFlowHistory(String projContain,
      String flowContains, String userNameContains, int status, long startTime,
      long endTime, int skip, int num) throws ExecutorManagerException {
    String query = FetchExecutableFlows.FETCH_BASE_EXECUTABLE_FLOW_QUERY;
    ArrayList<Object> params = new ArrayList<Object>();

    boolean first = true;
    if (projContain != null && !projContain.isEmpty()) {
      query += " ef JOIN projects p ON ef.project_id = p.id WHERE name LIKE ?";
      params.add('%' + projContain + '%');
      first = false;
    }

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

    QueryRunner runner = createQueryRunner();
    FetchExecutableFlows flowHandler = new FetchExecutableFlows();

    try {
      List<ExecutableFlow> properties =
          runner.query(query, flowHandler, params.toArray());
      return properties;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @Override
  public void addActiveExecutableReference(ExecutionReference reference)
      throws ExecutorManagerException {
    final String INSERT =
        "INSERT INTO active_executing_flows "
            + "(exec_id, update_time) values (?,?)";
    QueryRunner runner = createQueryRunner();

    try {
      runner.update(INSERT, reference.getExecId(), reference.getUpdateTime());
    } catch (SQLException e) {
      throw new ExecutorManagerException(
          "Error updating active flow reference " + reference.getExecId(), e);
    }
  }

  @Override
  public void removeActiveExecutableReference(int execid)
      throws ExecutorManagerException {
    final String DELETE = "DELETE FROM active_executing_flows WHERE exec_id=?";

    QueryRunner runner = createQueryRunner();
    try {
      runner.update(DELETE, execid);
    } catch (SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execid, e);
    }
  }

  @Override
  public boolean updateExecutableReference(int execId, long updateTime)
      throws ExecutorManagerException {
    final String DELETE =
        "UPDATE active_executing_flows set update_time=? WHERE exec_id=?";

    QueryRunner runner = createQueryRunner();
    int updateNum = 0;
    try {
      updateNum = runner.update(DELETE, updateTime, execId);
    } catch (SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execId, e);
    }

    // Should be 1.
    return updateNum > 0;
  }

  @Override
  public void uploadExecutableNode(ExecutableNode node, Props inputProps)
      throws ExecutorManagerException {
    final String INSERT_EXECUTION_NODE =
        "INSERT INTO execution_jobs "
            + "(exec_id, project_id, version, flow_id, job_id, start_time, "
            + "end_time, status, input_params, attempt) VALUES (?,?,?,?,?,?,?,?,?,?)";

    byte[] inputParam = null;
    if (inputProps != null) {
      try {
        String jsonString =
            JSONUtils.toJSON(PropsUtils.toHierarchicalMap(inputProps));
        inputParam = GZIPUtils.gzipString(jsonString, "UTF-8");
      } catch (IOException e) {
        throw new ExecutorManagerException("Error encoding input params");
      }
    }

    ExecutableFlow flow = node.getExecutableFlow();
    String flowId = node.getParentFlow().getFlowPath();
    System.out.println("Uploading flowId " + flowId);
    QueryRunner runner = createQueryRunner();
    try {
      runner.update(INSERT_EXECUTION_NODE, flow.getExecutionId(),
          flow.getProjectId(), flow.getVersion(), flowId, node.getId(),
          node.getStartTime(), node.getEndTime(), node.getStatus().getNumVal(),
          inputParam, node.getAttempt());
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error writing job " + node.getId(), e);
    }
  }

  @Override
  public void updateExecutableNode(ExecutableNode node)
      throws ExecutorManagerException {
    final String UPSERT_EXECUTION_NODE =
        "UPDATE execution_jobs "
            + "SET start_time=?, end_time=?, status=?, output_params=? "
            + "WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";

    byte[] outputParam = null;
    Props outputProps = node.getOutputProps();
    if (outputProps != null) {
      try {
        String jsonString =
            JSONUtils.toJSON(PropsUtils.toHierarchicalMap(outputProps));
        outputParam = GZIPUtils.gzipString(jsonString, "UTF-8");
      } catch (IOException e) {
        throw new ExecutorManagerException("Error encoding input params");
      }
    }

    QueryRunner runner = createQueryRunner();
    try {
      runner.update(UPSERT_EXECUTION_NODE, node.getStartTime(), node
          .getEndTime(), node.getStatus().getNumVal(), outputParam, node
          .getExecutableFlow().getExecutionId(), node.getParentFlow()
          .getFlowPath(), node.getId(), node.getAttempt());
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error updating job " + node.getId(),
          e);
    }
  }

  @Override
  public List<ExecutableJobInfo> fetchJobInfoAttempts(int execId, String jobId)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    try {
      List<ExecutableJobInfo> info =
          runner.query(
              FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE_ATTEMPTS,
              new FetchExecutableJobHandler(), execId, jobId);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  @Override
  public ExecutableJobInfo fetchJobInfo(int execId, String jobId, int attempts)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    try {
      List<ExecutableJobInfo> info =
          runner.query(FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE,
              new FetchExecutableJobHandler(), execId, jobId, attempts);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info.get(0);
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  @Override
  public Props fetchExecutionJobInputProps(int execId, String jobId)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    try {
      Pair<Props, Props> props =
          runner.query(
              FetchExecutableJobPropsHandler.FETCH_INPUT_PARAM_EXECUTABLE_NODE,
              new FetchExecutableJobPropsHandler(), execId, jobId);
      return props.getFirst();
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  @Override
  public Props fetchExecutionJobOutputProps(int execId, String jobId)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    try {
      Pair<Props, Props> props =
          runner
              .query(
                  FetchExecutableJobPropsHandler.FETCH_OUTPUT_PARAM_EXECUTABLE_NODE,
                  new FetchExecutableJobPropsHandler(), execId, jobId);
      return props.getFirst();
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  @Override
  public Pair<Props, Props> fetchExecutionJobProps(int execId, String jobId)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    try {
      Pair<Props, Props> props =
          runner
              .query(
                  FetchExecutableJobPropsHandler.FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE,
                  new FetchExecutableJobPropsHandler(), execId, jobId);
      return props;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  @Override
  public List<ExecutableJobInfo> fetchJobHistory(int projectId, String jobId,
      int skip, int size) throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    try {
      List<ExecutableJobInfo> info =
          runner.query(FetchExecutableJobHandler.FETCH_PROJECT_EXECUTABLE_NODE,
              new FetchExecutableJobHandler(), projectId, jobId, skip, size);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  @Override
  public LogData fetchLogs(int execId, String name, int attempt, int startByte,
      int length) throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    FetchLogsHandler handler =
        new FetchLogsHandler(startByte, length + startByte);
    try {
      LogData result =
          runner.query(FetchLogsHandler.FETCH_LOGS, handler, execId, name,
              attempt, startByte, startByte + length);
      return result;
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error fetching logs " + execId
          + " : " + name, e);
    }
  }

  @Override
  public List<Object> fetchAttachments(int execId, String jobId, int attempt)
      throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    try {
      String attachments =
          runner
              .query(
                  FetchExecutableJobAttachmentsHandler.FETCH_ATTACHMENTS_EXECUTABLE_NODE,
                  new FetchExecutableJobAttachmentsHandler(), execId, jobId);
      if (attachments == null) {
        return null;
      }

      @SuppressWarnings("unchecked")
      List<Object> attachmentList =
          (List<Object>) JSONUtils.parseJSONFromString(attachments);

      return attachmentList;
    } catch (IOException e) {
      throw new ExecutorManagerException(
          "Error converting job attachments to JSON " + jobId, e);
    } catch (SQLException e) {
      throw new ExecutorManagerException(
          "Error query job attachments " + jobId, e);
    }
  }

  @Override
  public void uploadLogFile(int execId, String name, int attempt, File... files)
      throws ExecutorManagerException {
    Connection connection = getConnection();
    try {
      uploadLogFile(connection, execId, name, attempt, files,
          defaultEncodingType);
      connection.commit();
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error committing log", e);
    } catch (IOException e) {
      throw new ExecutorManagerException("Error committing log", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void uploadLogFile(Connection connection, int execId, String name,
      int attempt, File[] files, EncodingType encType)
      throws ExecutorManagerException, IOException {
    // 50K buffer... if logs are greater than this, we chunk.
    // However, we better prevent large log files from being uploaded somehow
    byte[] buffer = new byte[50 * 1024];
    int pos = 0;
    int length = buffer.length;
    int startByte = 0;
    try {
      for (int i = 0; i < files.length; ++i) {
        File file = files[i];

        BufferedInputStream bufferedStream =
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
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error writing log part.", e);
    } catch (IOException e) {
      throw new ExecutorManagerException("Error chunking", e);
    }
  }

  private void uploadLogPart(Connection connection, int execId, String name,
      int attempt, int startByte, int endByte, EncodingType encType,
      byte[] buffer, int length) throws SQLException, IOException {
    final String INSERT_EXECUTION_LOGS =
        "INSERT INTO execution_logs "
            + "(exec_id, name, attempt, enc_type, start_byte, end_byte, "
            + "log, upload_time) VALUES (?,?,?,?,?,?,?,?)";

    QueryRunner runner = new QueryRunner();
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
  public void uploadAttachmentFile(ExecutableNode node, File file)
      throws ExecutorManagerException {
    Connection connection = getConnection();
    try {
      uploadAttachmentFile(connection, node, file, defaultEncodingType);
      connection.commit();
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error committing attachments ", e);
    } catch (IOException e) {
      throw new ExecutorManagerException("Error uploading attachments ", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void uploadAttachmentFile(Connection connection, ExecutableNode node,
      File file, EncodingType encType) throws SQLException, IOException {

    String jsonString = FileUtils.readFileToString(file);
    byte[] attachments = GZIPUtils.gzipString(jsonString, "UTF-8");

    final String UPDATE_EXECUTION_NODE_ATTACHMENTS =
        "UPDATE execution_jobs " + "SET attachments=? "
            + "WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";

    QueryRunner runner = new QueryRunner();
    runner.update(connection, UPDATE_EXECUTION_NODE_ATTACHMENTS, attachments,
        node.getExecutableFlow().getExecutionId(), node.getParentFlow()
            .getNestedId(), node.getId(), node.getAttempt());
  }

  private Connection getConnection() throws ExecutorManagerException {
    Connection connection = null;
    try {
      connection = super.getDBConnection(false);
    } catch (Exception e) {
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
    QueryRunner runner = createQueryRunner();
    FetchExecutorHandler executorHandler = new FetchExecutorHandler();

    try {
      List<Executor> executors =
        runner.query(FetchExecutorHandler.FETCH_ALL_EXECUTORS, executorHandler);
      return executors;
    } catch (Exception e) {
      throw new ExecutorManagerException("Error fetching executors", e);
    }
  }

  /**
   *
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchActiveExecutors()
   */
  @Override
  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    FetchExecutorHandler executorHandler = new FetchExecutorHandler();

    try {
      List<Executor> executors =
        runner.query(FetchExecutorHandler.FETCH_ACTIVE_EXECUTORS,
          executorHandler);
      return executors;
    } catch (Exception e) {
      throw new ExecutorManagerException("Error fetching active executors", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchExecutor(java.lang.String, int)
   */
  @Override
  public Executor fetchExecutor(String host, int port)
    throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    FetchExecutorHandler executorHandler = new FetchExecutorHandler();

    try {
      List<Executor> executors =
        runner.query(FetchExecutorHandler.FETCH_EXECUTOR_BY_HOST_PORT,
          executorHandler, host, port);
      if (executors.isEmpty()) {
        return null;
      } else {
        return executors.get(0);
      }
    } catch (Exception e) {
      throw new ExecutorManagerException(String.format(
        "Error fetching executor %s:%d", host, port), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#fetchExecutor(int)
   */
  @Override
  public Executor fetchExecutor(int executorId) throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    FetchExecutorHandler executorHandler = new FetchExecutorHandler();

    try {
      List<Executor> executors =
        runner.query(FetchExecutorHandler.FETCH_EXECUTOR_BY_ID,
          executorHandler, executorId);
      if (executors.isEmpty()) {
        return null;
      } else {
        return executors.get(0);
      }
    } catch (Exception e) {
      throw new ExecutorManagerException(String.format(
        "Error fetching executor with id: %d", executorId), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#updateExecutor(int)
   */
  @Override
  public void updateExecutor(Executor executor) throws ExecutorManagerException {
    final String UPDATE =
      "UPDATE executors SET host=?, port=?, active=? where id=?";

    QueryRunner runner = createQueryRunner();
    try {
      int rows =
        runner.update(UPDATE, executor.getHost(), executor.getPort(),
          executor.isActive(), executor.getId());
      if (rows == 0) {
        throw new ExecutorManagerException("No executor with id :"
          + executor.getId());
      }
    } catch (SQLException e) {
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
  public Executor addExecutor(String host, int port)
    throws ExecutorManagerException {
    // verify, if executor already exists
    Executor executor = fetchExecutor(host, port);
    if (executor != null) {
      throw new ExecutorManagerException(String.format(
        "Executor %s:%d already exist", host, port));
    }
    // add new executor
    addExecutorHelper(host, port);
    // fetch newly added executor
    executor = fetchExecutor(host, port);

    return executor;
  }

  private void addExecutorHelper(String host, int port)
    throws ExecutorManagerException {
    final String INSERT = "INSERT INTO executors (host, port) values (?,?)";
    QueryRunner runner = createQueryRunner();
    try {
      runner.update(INSERT, host, port);
    } catch (SQLException e) {
      throw new ExecutorManagerException(String.format("Error adding %s:%d ",
        host, port), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.executor.ExecutorLoader#postExecutorEvent(azkaban.executor.Executor,
   *      azkaban.executor.ExecutorLogEvent.EventType, java.lang.String,
   *      java.lang.String)
   */
  @Override
  public void postExecutorEvent(Executor executor, EventType type, String user,
    String message) throws ExecutorManagerException{
    QueryRunner runner = createQueryRunner();

    final String INSERT_PROJECT_EVENTS =
      "INSERT INTO executor_events (executor_id, event_type, event_time, username, message) values (?,?,?,?,?)";
    Date updateDate = new Date();
    try {
      runner.update(INSERT_PROJECT_EVENTS, executor.getId(), type.getNumVal(),
        updateDate, user, message);
    } catch (SQLException e) {
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
  public List<ExecutorLogEvent> getExecutorEvents(Executor executor, int num,
    int offset) throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();

    ExecutorLogsResultHandler logHandler = new ExecutorLogsResultHandler();
    List<ExecutorLogEvent> events = null;
    try {
      events =
        runner.query(ExecutorLogsResultHandler.SELECT_EXECUTOR_EVENTS_ORDER,
          logHandler, executor.getId(), num, offset);
    } catch (SQLException e) {
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
  public void assignExecutor(int executorId, int executionId)
    throws ExecutorManagerException {
    final String UPDATE =
      "UPDATE execution_flows SET executor_id=? where exec_id=?";

    QueryRunner runner = createQueryRunner();
    try {
      Executor executor = fetchExecutor(executorId);
      if (executor == null) {
        throw new ExecutorManagerException(String.format(
          "Failed to assign non-existent executor Id: %d to execution : %d  ",
          executorId, executionId));
      }

      int rows = runner.update(UPDATE, executorId, executionId);
      if (rows == 0) {
        throw new ExecutorManagerException(String.format(
          "Failed to assign executor Id: %d to non-existent execution : %d  ",
          executorId, executionId));
      }
    } catch (SQLException e) {
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
  public Executor fetchExecutorByExecutionId(int executionId)
    throws ExecutorManagerException {
    QueryRunner runner = createQueryRunner();
    FetchExecutorHandler executorHandler = new FetchExecutorHandler();
    Executor executor = null;
    try {
      List<Executor> executors =
        runner.query(FetchExecutorHandler.FETCH_EXECUTION_EXECUTOR,
          executorHandler, executionId);
      if (executors.size() > 0) {
        executor = executors.get(0);
      }
    } catch (SQLException e) {
      throw new ExecutorManagerException(
        "Error fetching executor for exec_id : " + executionId, e);
    }
    return executor;
  }

  private static class LastInsertID implements ResultSetHandler<Long> {
    private static String LAST_INSERT_ID = "SELECT LAST_INSERT_ID()";

    @Override
    public Long handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return -1L;
      }
      long id = rs.getLong(1);
      return id;
    }
  }

  private static class FetchLogsHandler implements ResultSetHandler<LogData> {
    private static String FETCH_LOGS =
        "SELECT exec_id, name, attempt, enc_type, start_byte, end_byte, log "
            + "FROM execution_logs "
            + "WHERE exec_id=? AND name=? AND attempt=? AND end_byte > ? "
            + "AND start_byte <= ? ORDER BY start_byte";

    private int startByte;
    private int endByte;

    public FetchLogsHandler(int startByte, int endByte) {
      this.startByte = startByte;
      this.endByte = endByte;
    }

    @Override
    public LogData handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      do {
        // int execId = rs.getInt(1);
        // String name = rs.getString(2);
        @SuppressWarnings("unused")
        int attempt = rs.getInt(3);
        EncodingType encType = EncodingType.fromInteger(rs.getInt(4));
        int startByte = rs.getInt(5);
        int endByte = rs.getInt(6);

        byte[] data = rs.getBytes(7);

        int offset =
            this.startByte > startByte ? this.startByte - startByte : 0;
        int length =
            this.endByte < endByte ? this.endByte - startByte - offset
                : endByte - startByte - offset;
        try {
          byte[] buffer = data;
          if (encType == EncodingType.GZIP) {
            buffer = GZIPUtils.unGzipBytes(data);
          }

          byteStream.write(buffer, offset, length);
        } catch (IOException e) {
          throw new SQLException(e);
        }
      } while (rs.next());

      byte[] buffer = byteStream.toByteArray();
      Pair<Integer, Integer> result =
          FileIOUtils.getUtf8Range(buffer, 0, buffer.length);

      return new LogData(startByte + result.getFirst(), result.getSecond(),
          new String(buffer, result.getFirst(), result.getSecond()));
    }
  }

  private static class FetchExecutableJobHandler implements
      ResultSetHandler<List<ExecutableJobInfo>> {
    private static String FETCH_EXECUTABLE_NODE =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt "
            + "FROM execution_jobs WHERE exec_id=? "
            + "AND job_id=? AND attempt_id=?";
    private static String FETCH_EXECUTABLE_NODE_ATTEMPTS =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt FROM execution_jobs "
            + "WHERE exec_id=? AND job_id=?";
    private static String FETCH_PROJECT_EXECUTABLE_NODE =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt FROM execution_jobs "
            + "WHERE project_id=? AND job_id=? "
            + "ORDER BY exec_id DESC LIMIT ?, ? ";

    @Override
    public List<ExecutableJobInfo> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutableJobInfo> emptyList();
      }

      List<ExecutableJobInfo> execNodes = new ArrayList<ExecutableJobInfo>();
      do {
        int execId = rs.getInt(1);
        int projectId = rs.getInt(2);
        int version = rs.getInt(3);
        String flowId = rs.getString(4);
        String jobId = rs.getString(5);
        long startTime = rs.getLong(6);
        long endTime = rs.getLong(7);
        Status status = Status.fromInteger(rs.getInt(8));
        int attempt = rs.getInt(9);

        ExecutableJobInfo info =
            new ExecutableJobInfo(execId, projectId, version, flowId, jobId,
                startTime, endTime, status, attempt);
        execNodes.add(info);
      } while (rs.next());

      return execNodes;
    }
  }

  private static class FetchExecutableJobAttachmentsHandler implements
      ResultSetHandler<String> {
    private static String FETCH_ATTACHMENTS_EXECUTABLE_NODE =
        "SELECT attachments FROM execution_jobs WHERE exec_id=? AND job_id=?";

    @Override
    public String handle(ResultSet rs) throws SQLException {
      String attachmentsJson = null;
      if (rs.next()) {
        try {
          byte[] attachments = rs.getBytes(1);
          if (attachments != null) {
            attachmentsJson = GZIPUtils.unGzipString(attachments, "UTF-8");
          }
        } catch (IOException e) {
          throw new SQLException("Error decoding job attachments", e);
        }
      }
      return attachmentsJson;
    }
  }

  private static class FetchExecutableJobPropsHandler implements
      ResultSetHandler<Pair<Props, Props>> {
    private static String FETCH_OUTPUT_PARAM_EXECUTABLE_NODE =
        "SELECT output_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
    private static String FETCH_INPUT_PARAM_EXECUTABLE_NODE =
        "SELECT input_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
    private static String FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE =
        "SELECT input_params, output_params "
            + "FROM execution_jobs WHERE exec_id=? AND job_id=?";

    @SuppressWarnings("unchecked")
    @Override
    public Pair<Props, Props> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return new Pair<Props, Props>(null, null);
      }

      if (rs.getMetaData().getColumnCount() > 1) {
        byte[] input = rs.getBytes(1);
        byte[] output = rs.getBytes(2);

        Props inputProps = null;
        Props outputProps = null;
        try {
          if (input != null) {
            String jsonInputString = GZIPUtils.unGzipString(input, "UTF-8");
            inputProps =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonInputString));

          }
          if (output != null) {
            String jsonOutputString = GZIPUtils.unGzipString(output, "UTF-8");
            outputProps =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonOutputString));
          }
        } catch (IOException e) {
          throw new SQLException("Error decoding param data", e);
        }

        return new Pair<Props, Props>(inputProps, outputProps);
      } else {
        byte[] params = rs.getBytes(1);
        Props props = null;
        try {
          if (params != null) {
            String jsonProps = GZIPUtils.unGzipString(params, "UTF-8");

            props =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonProps));
          }
        } catch (IOException e) {
          throw new SQLException("Error decoding param data", e);
        }

        return new Pair<Props, Props>(props, null);
      }
    }
  }

  /**
   * JDBC ResultSetHandler to fetch queued executions
   */
  private static class FetchQueuedExecutableFlows implements
    ResultSetHandler<List<Pair<ExecutionReference, ExecutableFlow>>> {
    // Select queued unassigned flows
    private static String FETCH_QUEUED_EXECUTABLE_FLOW =
      "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, "
        + " ax.update_time axUpdateTime FROM execution_flows ex"
        + " INNER JOIN"
        + " active_executing_flows ax ON ex.exec_id = ax.exec_id"
        + " Where ex.executor_id is NULL";

    @Override
    public List<Pair<ExecutionReference, ExecutableFlow>> handle(ResultSet rs)
      throws SQLException {
      if (!rs.next()) {
        return Collections
          .<Pair<ExecutionReference, ExecutableFlow>> emptyList();
      }

      List<Pair<ExecutionReference, ExecutableFlow>> execFlows =
        new ArrayList<Pair<ExecutionReference, ExecutableFlow>>();
      do {
        int id = rs.getInt(1);
        int encodingType = rs.getInt(2);
        byte[] data = rs.getBytes(3);
        long updateTime = rs.getLong(4);

        if (data == null) {
          logger.error("Found a flow with empty data blob exec_id: " + id);
        } else {
          EncodingType encType = EncodingType.fromInteger(encodingType);
          Object flowObj;
          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              String jsonString = new String(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            }

            ExecutableFlow exFlow =
              ExecutableFlow.createExecutableFlowFromObject(flowObj);
            ExecutionReference ref = new ExecutionReference(id);
            ref.setUpdateTime(updateTime);

            execFlows.add(new Pair<ExecutionReference, ExecutableFlow>(ref,
              exFlow));
          } catch (IOException e) {
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
    private static String FETCH_ACTIVE_EXECUTABLE_FLOW =
      "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
        + "et.port port, ax.update_time axUpdateTime, et.id executorId, et.active executorStatus"
        + " FROM execution_flows ex"
        + " INNER JOIN "
        + " active_executing_flows ax ON ex.exec_id = ax.exec_id"
        + " INNER JOIN "
        + " executors et ON ex.executor_id = et.id";

    @Override
    public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> handle(
        ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections
            .<Integer, Pair<ExecutionReference, ExecutableFlow>> emptyMap();
      }

      Map<Integer, Pair<ExecutionReference, ExecutableFlow>> execFlows =
          new HashMap<Integer, Pair<ExecutionReference, ExecutableFlow>>();
      do {
        int id = rs.getInt(1);
        int encodingType = rs.getInt(2);
        byte[] data = rs.getBytes(3);
        String host = rs.getString(4);
        int port = rs.getInt(5);
        long updateTime = rs.getLong(6);
        int executorId = rs.getInt(7);
        boolean executorStatus = rs.getBoolean(8);

        if (data == null) {
          execFlows.put(id, null);
        } else {
          EncodingType encType = EncodingType.fromInteger(encodingType);
          Object flowObj;
          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              String jsonString = new String(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            }

            ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(flowObj);
            Executor executor = new Executor(executorId, host, port, executorStatus);
            ExecutionReference ref = new ExecutionReference(id, executor);
            ref.setUpdateTime(updateTime);

            execFlows.put(id, new Pair<ExecutionReference, ExecutableFlow>(ref,
                exFlow));
          } catch (IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }

  private static class FetchExecutableFlows implements
      ResultSetHandler<List<ExecutableFlow>> {
    private static String FETCH_BASE_EXECUTABLE_FLOW_QUERY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows ";
    private static String FETCH_EXECUTABLE_FLOW =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE exec_id=?";
    // private static String FETCH_ACTIVE_EXECUTABLE_FLOW =
    // "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data "
    // +
    // "FROM execution_flows ex " +
    // "INNER JOIN active_executing_flows ax ON ex.exec_id = ax.exec_id";
    private static String FETCH_ALL_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    private static String FETCH_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    private static String FETCH_EXECUTABLE_FLOW_BY_STATUS =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? AND status=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";

    @Override
    public List<ExecutableFlow> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutableFlow> emptyList();
      }

      List<ExecutableFlow> execFlows = new ArrayList<ExecutableFlow>();
      do {
        int id = rs.getInt(1);
        int encodingType = rs.getInt(2);
        byte[] data = rs.getBytes(3);

        if (data != null) {
          EncodingType encType = EncodingType.fromInteger(encodingType);
          Object flowObj;
          try {
            // Convoluted way to inflate strings. Should find common package
            // or helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              String jsonString = new String(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            }

            ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(flowObj);
            execFlows.add(exFlow);
          } catch (IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }

  private static class IntHandler implements ResultSetHandler<Integer> {
    private static String NUM_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_flows";
    private static String NUM_FLOW_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_flows WHERE project_id=? AND flow_id=?";
    private static String NUM_JOB_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_jobs WHERE project_id=? AND job_id=?";
    private static String FETCH_EXECUTOR_ID =
        "SELECT executor_id FROM execution_flows WHERE exec_id=?";

    @Override
    public Integer handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }

  @Override
  public int removeExecutionLogsByTime(long millis)
      throws ExecutorManagerException {
    final String DELETE_BY_TIME =
        "DELETE FROM execution_logs WHERE upload_time < ?";

    QueryRunner runner = createQueryRunner();
    int updateNum = 0;
    try {
      updateNum = runner.update(DELETE_BY_TIME, millis);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new ExecutorManagerException(
          "Error deleting old execution_logs before " + millis, e);
    }

    return updateNum;
  }

  /**
   * JDBC ResultSetHandler to fetch records from executors table
   */
  private static class FetchExecutorHandler implements
    ResultSetHandler<List<Executor>> {
    private static String FETCH_ALL_EXECUTORS =
      "SELECT id, host, port, active FROM executors";
    private static String FETCH_ACTIVE_EXECUTORS =
      "SELECT id, host, port, active FROM executors where active=true";
    private static String FETCH_EXECUTOR_BY_ID =
      "SELECT id, host, port, active FROM executors where id=?";
    private static String FETCH_EXECUTOR_BY_HOST_PORT =
      "SELECT id, host, port, active FROM executors where host=? AND port=?";
    private static String FETCH_EXECUTION_EXECUTOR =
      "SELECT ex.id, ex.host, ex.port, ex.active FROM "
        + " executors ex INNER JOIN execution_flows ef "
        + "on ex.id = ef.executor_id  where exec_id=?";

    @Override
    public List<Executor> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Executor> emptyList();
      }

      List<Executor> executors = new ArrayList<Executor>();
      do {
        int id = rs.getInt(1);
        String host = rs.getString(2);
        int port = rs.getInt(3);
        boolean active = rs.getBoolean(4);
        Executor executor = new Executor(id, host, port, active);
        executors.add(executor);
      } while (rs.next());

      return executors;
    }
  }

  /**
   * JDBC ResultSetHandler to fetch records from executor_events table
   */
  private static class ExecutorLogsResultHandler implements
    ResultSetHandler<List<ExecutorLogEvent>> {
    private static String SELECT_EXECUTOR_EVENTS_ORDER =
      "SELECT executor_id, event_type, event_time, username, message FROM executor_events "
        + " WHERE executor_id=? ORDER BY event_time LIMIT ? OFFSET ?";

    @Override
    public List<ExecutorLogEvent> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutorLogEvent> emptyList();
      }

      ArrayList<ExecutorLogEvent> events = new ArrayList<ExecutorLogEvent>();
      do {
        int executorId = rs.getInt(1);
        int eventType = rs.getInt(2);
        Date eventTime = rs.getDate(3);
        String username = rs.getString(4);
        String message = rs.getString(5);

        ExecutorLogEvent event =
          new ExecutorLogEvent(executorId, username, eventTime,
            EventType.fromInteger(eventType), message);
        events.add(event);
      } while (rs.next());

      return events;
    }
  }

  /**
   *
   * {@inheritDoc}
   * @see azkaban.executor.ExecutorLoader#unassignExecutor(int)
   */
  @Override
  public void unassignExecutor(int executionId) throws ExecutorManagerException {
    final String UPDATE =
      "UPDATE execution_flows SET executor_id=NULL where exec_id=?";

    QueryRunner runner = createQueryRunner();
    try {
      int rows = runner.update(UPDATE, executionId);
      if (rows == 0) {
        throw new ExecutorManagerException(String.format(
          "Failed to unassign executor for execution : %d  ", executionId));
      }
    } catch (SQLException e) {
      throw new ExecutorManagerException("Error updating execution id "
        + executionId, e);
    }
  }
}

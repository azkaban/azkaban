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

package azkaban.executor;

import azkaban.db.DatabaseOperator;
import azkaban.db.EncodingType;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.GZIPUtils;
import azkaban.utils.Pair;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

@Singleton
public class FetchActiveFlowDao {

  private static final Logger logger = Logger.getLogger(FetchActiveFlowDao.class);

  private final DatabaseOperator dbOperator;

  @Inject
  public FetchActiveFlowDao(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  private static Pair<ExecutionReference, ExecutableFlow> getExecutableFlowHelper(
      final ResultSet rs) throws SQLException {
    final int id = rs.getInt("exec_id");
    final int encodingType = rs.getInt("enc_type");
    final byte[] data = rs.getBytes("flow_data");

    if (data == null) {
      logger.warn("Execution id " + id + " has flow_data = null. To clean up, update status to "
          + "FAILED manually, eg. "
          + "SET status = " + Status.FAILED.getNumVal() + " WHERE id = " + id);
    } else {
      final EncodingType encType = EncodingType.fromInteger(encodingType);
      final ExecutableFlow exFlow;
      try {
        exFlow = ExecutableFlow.createExecutableFlowFromObject(
            GZIPUtils.transformBytesToObject(data, encType));
      } catch (final IOException e) {
        throw new SQLException("Error retrieving flow data " + id, e);
      }
      return getPairWithExecutorInfo(rs, exFlow);
    }
    return null;
  }

  private static Pair<ExecutionReference, ExecutableFlow> getPairWithExecutorInfo(final ResultSet rs,
      final ExecutableFlow exFlow) throws SQLException {
    final int executorId = rs.getInt("executorId");
    final String host = rs.getString("host");
    final int port = rs.getInt("port");
    final Executor executor;
    if (host == null) {
      logger.warn("Executor id " + executorId + " (on execution " +
          exFlow.getExecutionId() + ") wasn't found");
      executor = null;
    } else {
      final boolean executorStatus = rs.getBoolean("executorStatus");
      executor = new Executor(executorId, host, port, executorStatus);
    }
    final ExecutionReference ref = new ExecutionReference(exFlow.getExecutionId(), executor);
    return new Pair<>(ref, exFlow);
  }

  private static Pair<ExecutionReference, ExecutableFlow> getExecutionHelper(
      final ResultSet rs) throws SQLException {
    final Flow flow = new Flow(rs.getString("flow_id"));
    final Project project = new Project(rs.getInt("project_id"), null);
    project.setVersion(rs.getInt("version"));
    final ExecutableFlow exFlow = new ExecutableFlow(project, flow);
    exFlow.setExecutionId(rs.getInt("exec_id"));
    exFlow.setStatus(Status.fromInteger(rs.getInt("status")));
    exFlow.setSubmitTime(rs.getLong("submit_time"));
    exFlow.setStartTime(rs.getLong("start_time"));
    exFlow.setEndTime(rs.getLong("end_time"));
    return getPairWithExecutorInfo(rs, exFlow);
  }

  /**
   * Fetch flows that are not in finished status, including both dispatched and non-dispatched
   * flows.
   *
   * @return unfinished flows map
   * @throws ExecutorManagerException the executor manager exception
   */
  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchUnfinishedFlows()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchActiveExecutableFlows.FETCH_UNFINISHED_EXECUTABLE_FLOWS,
          new FetchActiveExecutableFlows());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching unfinished flows", e);
    }
  }

  /**
   * Fetch unfinished executions as {@link #fetchUnfinishedFlows}, excluding flow data.
   *
   * @return unfinished flows map
   * @throws ExecutorManagerException the executor manager exception
   */
  public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchUnfinishedExecutions()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchActiveExecutions.FETCH_UNFINISHED_EXECUTIONS,
          new FetchActiveExecutions());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching unfinished flows", e);
    }
  }

  /**
   * Fetch flows that are dispatched and not yet finished.
   *
   * @return active flows map
   * @throws ExecutorManagerException the executor manager exception
   */
  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchActiveExecutableFlows.FETCH_ACTIVE_EXECUTABLE_FLOWS,
          new FetchActiveExecutableFlows());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  /**
   * Fetch the flow that is dispatched and not yet finished by execution id.
   *
   * @return active flow pair
   * @throws ExecutorManagerException the executor manager exception
   */
  Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(final int execId)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchActiveExecutableFlow
              .FETCH_ACTIVE_EXECUTABLE_FLOW_BY_EXEC_ID,
          new FetchActiveExecutableFlow(), execId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flow by exec id" + execId, e);
    }
  }

  @VisibleForTesting
  static class FetchActiveExecutableFlows implements
      ResultSetHandler<Map<Integer, Pair<ExecutionReference, ExecutableFlow>>> {

    // Select flows that are not in finished status
    private static final String FETCH_UNFINISHED_EXECUTABLE_FLOWS =
        "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
            + "et.port port, ex.executor_id executorId, et.active executorStatus"
            + " FROM execution_flows ex"
            + " LEFT JOIN "
            + " executors et ON ex.executor_id = et.id"
            + " Where ex.status NOT IN ("
            + Status.SUCCEEDED.getNumVal() + ", "
            + Status.KILLED.getNumVal() + ", "
            + Status.FAILED.getNumVal() + ")";

    // Select flows that are dispatched and not in finished status
    private static final String FETCH_ACTIVE_EXECUTABLE_FLOWS =
        "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
            + "et.port port, ex.executor_id executorId, et.active executorStatus"
            + " FROM execution_flows ex"
            + " LEFT JOIN "
            + " executors et ON ex.executor_id = et.id"
            + " Where ex.status NOT IN ("
            + Status.SUCCEEDED.getNumVal() + ", "
            + Status.KILLED.getNumVal() + ", "
            + Status.FAILED.getNumVal() + ")"
            // exclude queued flows that haven't been assigned yet -- this is the opposite of
            // the condition in ExecutionFlowDao#FETCH_QUEUED_EXECUTABLE_FLOW
            + " AND NOT ("
            + "   ex.executor_id IS NULL"
            + "   AND ex.status = " + Status.PREPARING.getNumVal()
            + " )";

    @Override
    public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> handle(
        final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyMap();
      }

      final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> execFlows =
          new HashMap<>();
      do {
        final Pair<ExecutionReference, ExecutableFlow> exFlow = getExecutableFlowHelper(rs);
        if (exFlow != null) {
          execFlows.put(rs.getInt("exec_id"), exFlow);
        }
      } while (rs.next());

      return execFlows;
    }
  }

  // TODO create unit test
  @VisibleForTesting
  static class FetchActiveExecutions implements
      ResultSetHandler<Map<Integer, Pair<ExecutionReference, ExecutableFlow>>> {

    // Select flows that are not in finished status
    private static final String FETCH_UNFINISHED_EXECUTIONS =
        "SELECT ex.exec_id exec_id, ex.project_id project_id, ex.version version, "
            + "ex.flow_id flow_id, et.host host, et.port port, ex.executor_id executorId, "
            + "ex.status status, ex.submit_time submit_time, ex.start_time start_time, "
            + "ex.end_time end_time, et.active executorStatus"
            + " FROM execution_flows ex"
            + " LEFT JOIN "
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
        final Pair<ExecutionReference, ExecutableFlow> exFlow = getExecutionHelper(rs);
        if (exFlow != null) {
          execFlows.put(rs.getInt("exec_id"), exFlow);
        }
      } while (rs.next());

      return execFlows;
    }
  }

  private static class FetchActiveExecutableFlow implements
      ResultSetHandler<Pair<ExecutionReference, ExecutableFlow>> {

    // Select the flow that is dispatched and not in finished status by execution id
    private static final String FETCH_ACTIVE_EXECUTABLE_FLOW_BY_EXEC_ID =
        "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
            + "et.port port, ex.executor_id executorId, et.active executorStatus"
            + " FROM execution_flows ex"
            + " LEFT JOIN "
            + " executors et ON ex.executor_id = et.id"
            + " Where ex.exec_id = ? AND ex.status NOT IN ("
            + Status.SUCCEEDED.getNumVal() + ", "
            + Status.KILLED.getNumVal() + ", "
            + Status.FAILED.getNumVal() + ")"
            // exclude queued flows that haven't been assigned yet -- this is the opposite of
            // the condition in ExecutionFlowDao#FETCH_QUEUED_EXECUTABLE_FLOW
            + " AND NOT ("
            + "   ex.executor_id IS NULL"
            + "   AND ex.status = " + Status.PREPARING.getNumVal()
            + " )";

    @Override
    public Pair<ExecutionReference, ExecutableFlow> handle(
        final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }
      return getExecutableFlowHelper(rs);
    }
  }

}

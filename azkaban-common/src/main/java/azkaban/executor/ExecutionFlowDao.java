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
import azkaban.db.SQLTransaction;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ExecutionFlowDao {

  private static final Logger logger = LoggerFactory.getLogger(ExecutionFlowDao.class);
  private final DatabaseOperator dbOperator;

  @Inject
  public ExecutionFlowDao(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  public void uploadExecutableFlow(final ExecutableFlow flow)
      throws ExecutorManagerException {

    final String useExecutorParam =
        flow.getExecutionOptions().getFlowParameters().get(ExecutionOptions.USE_EXECUTOR);
    final String executorId = StringUtils.isNotEmpty(useExecutorParam) ? useExecutorParam : null;

    final String flowPriorityParam =
        flow.getExecutionOptions().getFlowParameters().get(ExecutionOptions.FLOW_PRIORITY);
    final int flowPriority = StringUtils.isNotEmpty(flowPriorityParam) ?
        Integer.parseInt(flowPriorityParam) : ExecutionOptions.DEFAULT_FLOW_PRIORITY;

    final String INSERT_EXECUTABLE_FLOW = "INSERT INTO execution_flows "
        + "(project_id, flow_id, version, status, submit_time, submit_user, update_time, "
        + "use_executor, flow_priority) values (?,?,?,?,?,?,?,?,?)";
    final long submitTime = flow.getSubmitTime();

    /**
     * Why we need a transaction to get last insert ID?
     * Because "SELECT LAST_INSERT_ID()" needs to have the same connection
     * as inserting the new entry.
     * See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id
     */
    final SQLTransaction<Long> insertAndGetLastID = transOperator -> {
      transOperator.update(INSERT_EXECUTABLE_FLOW, flow.getProjectId(),
          flow.getFlowId(), flow.getVersion(), flow.getStatus().getNumVal(),
          submitTime, flow.getSubmitUser(), submitTime, executorId, flowPriority);
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

  List<ExecutableFlow> fetchFlowHistory(final int skip, final int num)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchExecutableFlows.FETCH_ALL_EXECUTABLE_FLOW_HISTORY,
          new FetchExecutableFlows(), skip, num);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching flow History", e);
    }
  }

  List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
      final int skip, final int num)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_HISTORY,
          new FetchExecutableFlows(), projectId, flowId, skip, num);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching flow history", e);
    }
  }

  public List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchQueuedExecutableFlows.FETCH_QUEUED_EXECUTABLE_FLOW,
          new FetchQueuedExecutableFlows());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  /**
   * fetch flow execution history with specified {@code projectId}, {@code flowId} and flow start
   * time >= {@code startTime}
   *
   * @return the list of flows meeting the specified criteria
   */
  public List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId, final
  long startTime) throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_BY_START_TIME,
          new FetchExecutableFlows(), projectId, flowId, startTime);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching historic flows", e);
    }
  }

  List<ExecutableFlow> fetchFlowHistory(final int projectId, final String flowId,
      final int skip, final int num, final Status status)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW_BY_STATUS,
          new FetchExecutableFlows(), projectId, flowId, status.getNumVal(), skip, num);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  List<ExecutableFlow> fetchRecentlyFinishedFlows(final Duration maxAge)
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

  List<ExecutableFlow> fetchFlowHistory(final String projContain, final String flowContains,
      final String userNameContains, final int status,
      final long startTime, final long endTime,
      final int skip, final int num)
      throws ExecutorManagerException {
    String query = FetchExecutableFlows.FETCH_BASE_EXECUTABLE_FLOW_QUERY;
    final List<Object> params = new ArrayList<>();

    boolean first = true;
    if (projContain != null && !projContain.isEmpty()) {
      query += " JOIN projects p ON ef.project_id = p.id WHERE name LIKE ?";
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

  void updateExecutableFlow(final ExecutableFlow flow) throws ExecutorManagerException {
    updateExecutableFlow(flow, EncodingType.GZIP);
  }

  private void updateExecutableFlow(final ExecutableFlow flow, final EncodingType encType)
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

  public ExecutableFlow fetchExecutableFlow(final int execId) throws ExecutorManagerException {
    final FetchExecutableFlows flowHandler = new FetchExecutableFlows();
    try {
      final List<ExecutableFlow> properties = this.dbOperator
          .query(FetchExecutableFlows.FETCH_EXECUTABLE_FLOW, flowHandler, execId);
      if (properties.isEmpty()) {
        return null;
      } else {
        return properties.get(0);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching flow id " + execId, e);
    }
  }

  /**
   * set executor id to null for the execution id
   */
  public void unsetExecutorIdForExecution(final int executionId) throws ExecutorManagerException {
    final String UNSET_EXECUTOR = "UPDATE execution_flows SET executor_id = null where exec_id = ?";

    final SQLTransaction<Integer> unsetExecutor =
        transOperator -> transOperator.update(UNSET_EXECUTOR, executionId);

    try {
      this.dbOperator.transaction(unsetExecutor);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error unsetting executor id for execution " + executionId,
          e);
    }
  }

  public ExecutableFlow selectAndUpdateExecution(final int executorId, final boolean isActive,
      final Predicate<ExecutableFlow> evaluateCandidate)
      throws ExecutorManagerException {
    final String UPDATE_EXECUTION = "UPDATE execution_flows SET executor_id = ? where exec_id = ?";
    final String selectExecutionForUpdate = isActive ?
        FetchExecutableFlows.SELECT_EXECUTION_FOR_UPDATE_ACTIVE :
        FetchExecutableFlows.SELECT_EXECUTION_FOR_UPDATE_INACTIVE;

    final SQLTransaction<ExecutableFlow> selectAndUpdateExecution = transOperator -> {
      final List<ExecutableFlow> execFlows = transOperator.query(selectExecutionForUpdate,
          new FetchExecutableFlows(), executorId);

      ExecutableFlow executableFlow = null;
      for (final ExecutableFlow candidateExecFlow : execFlows) {
        if (evaluateCandidate.test(candidateExecFlow)) {
          logger.info("Executor {} accepting execution {}", executorId,
              candidateExecFlow.getExecutionId());
          executableFlow = candidateExecFlow;
          transOperator.update(UPDATE_EXECUTION, executorId, executableFlow.getExecutionId());
          break;
        } else {
          logger.info("Executor {} rejecting execution {}", executorId,
              candidateExecFlow.getExecutionId());
        }
      }
      transOperator.getConnection().commit();
      return executableFlow;
    };

    try {
      return this.dbOperator.transaction(selectAndUpdateExecution);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error selecting and updating execution with executor "
          + executorId, e);
    }
  }

  public static class FetchExecutableFlows implements
      ResultSetHandler<List<ExecutableFlow>> {

    static String FETCH_EXECUTABLE_FLOW_BY_START_TIME =
        "SELECT ef.exec_id, ef.enc_type, ef.flow_data FROM execution_flows ef WHERE project_id=? "
            + "AND flow_id=? AND start_time >= ? ORDER BY start_time DESC";
    static String FETCH_BASE_EXECUTABLE_FLOW_QUERY =
        "SELECT ef.exec_id, ef.enc_type, ef.flow_data FROM execution_flows ef";
    static String FETCH_EXECUTABLE_FLOW =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE exec_id=?";
    static String FETCH_ALL_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    static String FETCH_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    static String FETCH_EXECUTABLE_FLOW_BY_STATUS =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? AND status=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";

    private static final String SELECT_EXECUTION_FOR_UPDATE_FORMAT =
        "SELECT exec_id, enc_type, flow_data from execution_flows"
            + " WHERE status = " + Status.PREPARING.getNumVal()
            + " and executor_id is NULL and flow_data is NOT NULL and %s"
            + " ORDER BY flow_priority DESC, submit_time ASC, exec_id ASC FOR UPDATE";

    static final String SELECT_EXECUTION_FOR_UPDATE_ACTIVE =
        String.format(SELECT_EXECUTION_FOR_UPDATE_FORMAT,
            "(use_executor is NULL or use_executor = ?)");

    static final String SELECT_EXECUTION_FOR_UPDATE_INACTIVE =
        String.format(SELECT_EXECUTION_FOR_UPDATE_FORMAT, "use_executor = ?");

    @Override
    public List<ExecutableFlow> handle(final ResultSet rs) throws SQLException {
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
          try {
            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(
                    GZIPUtils.transformBytesToObject(data, encType));
            execFlows.add(exFlow);
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
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
          ExecutionFlowDao.logger.error("Found a flow with empty data blob exec_id: " + id);
        } else {
          final EncodingType encType = EncodingType.fromInteger(encodingType);
          try {
            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(
                    GZIPUtils.transformBytesToObject(data, encType));
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
          try {
            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(
                    GZIPUtils.transformBytesToObject(data, encType));
            execFlows.add(exFlow);
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());
      return execFlows;
    }
  }
}

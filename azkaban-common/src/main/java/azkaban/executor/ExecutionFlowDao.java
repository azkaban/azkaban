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

import azkaban.DispatchMethod;
import azkaban.db.DatabaseOperator;
import azkaban.db.EncodingType;
import azkaban.db.SQLTransaction;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

@Singleton
public class ExecutionFlowDao {

  private static final Logger logger = Logger.getLogger(ExecutionFlowDao.class);
  private final DatabaseOperator dbOperator;
  private final MysqlNamedLock mysqlNamedLock;

  private static final String POLLING_LOCK_NAME = "execution_flows_polling";
  private static final int GET_LOCK_TIMEOUT_IN_SECONDS = 5;

  @Inject
  public ExecutionFlowDao(final DatabaseOperator dbOperator, final MysqlNamedLock mysqlNamedLock) {
    this.dbOperator = dbOperator;
    this.mysqlNamedLock = mysqlNamedLock;
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
        + "use_executor, flow_priority, execution_source, dispatch_method) values (?,?,?,?,?,?,?,"
        + "?,?,?,?)";
    final long submitTime = flow.getSubmitTime();
    final String executionSource = flow.getExecutionSource();
    final DispatchMethod dispatchMethod = flow.getDispatchMethod();

    /**
     * Why we need a transaction to get last insert ID?
     * Because "SELECT LAST_INSERT_ID()" needs to have the same connection
     * as inserting the new entry.
     * See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id
     */
    final SQLTransaction<Long> insertAndGetLastID = transOperator -> {
      transOperator.update(INSERT_EXECUTABLE_FLOW, flow.getProjectId(),
          flow.getFlowId(), flow.getVersion(), flow.getStatus().getNumVal(),
          submitTime, flow.getSubmitUser(), submitTime, executorId, flowPriority, executionSource
          , dispatchMethod.getNumVal());
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

  public List<ExecutableFlow> fetchAgedQueuedFlows(final Duration minAge)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchExecutableFlows.FETCH_FLOWS_QUEUED_FOR_LONG_TIME,
          new FetchExecutableFlows(), System.currentTimeMillis() - minAge.toMillis());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching aged queued flows", e);
    }
  }

  public List<Pair<ExecutionReference, ExecutableFlow>> fetchQueuedFlows(final Status status)
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchQueuedExecutableFlows.FETCH_QUEUED_EXECUTABLE_FLOW,
          new FetchQueuedExecutableFlows(), status.getNumVal());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  public List<ExecutableFlow> fetchStaleFlowsForStatus(final Status status,
      final ImmutableMap<Status, Pair<Duration, String>> validityMap)
      throws ExecutorManagerException {
    return fetchFlowsForStatusWithTimeValidity(status, validityMap, true);
  }


  public List<ExecutableFlow> fetchFreshFlowsForStatus(final Status status,
      final ImmutableMap<Status, Pair<Duration, String>> validityMap)
      throws ExecutorManagerException {
    return fetchFlowsForStatusWithTimeValidity(status, validityMap, false);
  }

  /**
   *
   * @param status the status of flows you want
   * @param validityMap contains the status as key, the value is a pair, defining timestamp by
   *                    subtracting a duration from now, and what event time (submit/start/update)
   *                    this timestamp is comparing with
   * @param looksForStale if true return flows is stale comparing to this timestamp, else find
   *                      flows fresher than this timestamp
   */
  List<ExecutableFlow> fetchFlowsForStatusWithTimeValidity(
      final Status status,
      final ImmutableMap<Status, Pair<Duration, String>> validityMap,
      boolean looksForStale)
      throws ExecutorManagerException {
    if (!validityMap.containsKey(status)) {
      throw new ExecutorManagerException(
          "Validity duration is not defined for status: " + status.name());
    }
    final Pair<Duration, String> validity = validityMap.get(status);
    final Duration validityDuration = validity.getFirst();
    final String validityFrom = validity.getSecond();
    final long beforeInMillis = System.currentTimeMillis() - validityDuration.toMillis();

    // For status PREPARING (20), to find stale flows, validityDuration is 15 Mins and validityFrom
    // is "submit_time", and they query will be:
    // SELECT ef.exec_id, ef.enc_type, ef.flow_data, ef.status FROM execution_flows ef
    // WHERE status=20 AND submit_time>0 AND submit_time<beforeInMillis
    final StringBuilder query = new StringBuilder()
        .append(FetchExecutableFlows.FETCH_BASE_EXECUTABLE_FLOW_QUERY)
        .append(" WHERE status=? ")
        .append(" AND dispatch_method=?")
        .append(" AND " + validityFrom + ">0")
        .append(" AND " + validityFrom + (looksForStale ? "<" : ">=") + "?");
    try {
      return this.dbOperator.query(query.toString(), new FetchExecutableFlows(),
          status.getNumVal(), DispatchMethod.CONTAINERIZED.getNumVal(), beforeInMillis);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching stale flows", e);
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

  List<ExecutableFlow> fetchFlowHistory(final String projectNameContains,
      final String flowNameContains, final String userNameContains, final int status,
      final long startTime, final long endTime, final int skip, final int num)
      throws ExecutorManagerException {
    String query = FetchExecutableFlows.FETCH_BASE_EXECUTABLE_FLOW_QUERY;
    final List<Object> params = new ArrayList<>();

    boolean first = true;
    if (projectNameContains != null && !projectNameContains.isEmpty()) {
      query += " JOIN projects p ON ef.project_id = p.id WHERE name LIKE ?";
      params.add('%' + projectNameContains + '%');
      first = false;
    }

    // todo kunkun-tang: we don't need the below complicated logics. We should just use a simple way.
    if (flowNameContains != null && !flowNameContains.isEmpty()) {
      if (first) {
        query += " WHERE ";
        first = false;
      } else {
        query += " AND ";
      }

      query += " flow_id LIKE ?";
      params.add('%' + flowNameContains + '%');
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

    byte[] data = null;
    try {
      // If this action fails, the execution must be failed.
      final String json = JSONUtils.toJSON(flow.toObject());
      final byte[] stringData = json.getBytes("UTF-8");
      data = stringData;
      // Todo kunkun-tang: use a common method to transform stringData to data.
      if (encType == EncodingType.GZIP) {
        data = GZIPUtils.gzipBytes(stringData);
      }
    } catch (final IOException e) {
      flow.setStatus(Status.FAILED);
      updateExecutableFlowStatusInDB(flow);
      throw new ExecutorManagerException("Error encoding the execution flow. Execution Id  = "
          + flow.getExecutionId());
    } catch (final RuntimeException re) {
      flow.setStatus(Status.FAILED);
      // Likely due to serialization error
      if ( data == null && re instanceof NullPointerException) {
        logger.warn("Failed to serialize executable flow for " + flow.getExecutionId());
        logger.warn("NPE stacktrace" + ExceptionUtils.getStackTrace(re));
      }
      updateExecutableFlowStatusInDB(flow);
      throw new ExecutorManagerException("Error encoding the execution flow due to "
          + "RuntimeException. Execution Id  = " + flow.getExecutionId(), re);
    }

    try {
      this.dbOperator.update(UPDATE_EXECUTABLE_FLOW_DATA, flow.getStatus()
          .getNumVal(), flow.getUpdateTime(), flow.getStartTime(), flow
          .getEndTime(), encType.getNumVal(), data, flow.getExecutionId());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating flow.", e);
    }
  }

  private void updateExecutableFlowStatusInDB(final ExecutableFlow flow)
    throws ExecutorManagerException {
    final String UPDATE_FLOW_STATUS = "UPDATE execution_flows SET status = ?, update_time = ? "
        + "where exec_id = ?";

    try {
      this.dbOperator.update(UPDATE_FLOW_STATUS, flow.getStatus().getNumVal(),
          System.currentTimeMillis(), flow.getExecutionId());
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
    final String UNSET_EXECUTOR = "UPDATE execution_flows SET executor_id = null, update_time = ? where exec_id = ?";

    final SQLTransaction<Integer> unsetExecutor =
        transOperator -> transOperator.update(UNSET_EXECUTOR, System.currentTimeMillis(),
            executionId);

    try {
      this.dbOperator.transaction(unsetExecutor);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error unsetting executor id for execution " + executionId,
          e);
    }
  }

  public int selectAndUpdateExecution(final int executorId, final boolean isActive,
      final DispatchMethod dispatchMethod)
      throws ExecutorManagerException {
    final String UPDATE_EXECUTION = "UPDATE execution_flows SET executor_id = ?, update_time = ? "
        + "where exec_id = ?";
    final String selectExecutionForUpdate = isActive ?
        SelectFromExecutionFlows.SELECT_EXECUTION_FOR_UPDATE_ACTIVE :
        SelectFromExecutionFlows.SELECT_EXECUTION_FOR_UPDATE_INACTIVE;

    final SQLTransaction<Integer> selectAndUpdateExecution = transOperator -> {
      transOperator.getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

      final List<Integer> execIds = transOperator.query(selectExecutionForUpdate,
          new SelectFromExecutionFlows(), Status.PREPARING.getNumVal(), dispatchMethod.getNumVal(),
              executorId);

      int execId = -1;
      if (!execIds.isEmpty()) {
        execId = execIds.get(0);
        transOperator.update(UPDATE_EXECUTION, executorId, System.currentTimeMillis(), execId);
      }
      transOperator.getConnection().commit();
      return execId;
    };

    try {
      return this.dbOperator.transaction(selectAndUpdateExecution);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error selecting and updating execution with executor "
          + executorId, e);
    }
  }

  public int selectAndUpdateExecutionWithLocking(final int executorId, final boolean isActive,
      final DispatchMethod dispatchMethod)
      throws ExecutorManagerException {
    final String UPDATE_EXECUTION = "UPDATE execution_flows SET executor_id = ?, update_time = ? "
        + "where exec_id = ?";
    final String selectExecutionForUpdate = isActive ?
        SelectFromExecutionFlows.SELECT_EXECUTION_FOR_UPDATE_ACTIVE :
        SelectFromExecutionFlows.SELECT_EXECUTION_FOR_UPDATE_INACTIVE;

    final SQLTransaction<Integer> selectAndUpdateExecution = transOperator -> {
      int execId = -1;
      final boolean hasLocked = this.mysqlNamedLock.getLock(transOperator, POLLING_LOCK_NAME, GET_LOCK_TIMEOUT_IN_SECONDS);
      logger.info("ExecutionFlow polling lock value: " + hasLocked + " for executorId: " + executorId);
      if (hasLocked) {
        try {
          final List<Integer> execIds = transOperator.query(selectExecutionForUpdate,
              new SelectFromExecutionFlows(), Status.PREPARING.getNumVal(), dispatchMethod.getNumVal(),
              executorId);
          if (CollectionUtils.isNotEmpty(execIds)) {
            execId = execIds.get(0);
            transOperator.update(UPDATE_EXECUTION, executorId, System.currentTimeMillis(), execId);
          }
        } finally {
          this.mysqlNamedLock.releaseLock(transOperator, POLLING_LOCK_NAME);
          logger.info("Released polling lock for executorId: " + executorId);
        }
      } else {
        logger.info("Could not acquire polling lock for executorId: " + executorId);
      }
      return execId;
    };

    try {
      return this.dbOperator.transaction(selectAndUpdateExecution);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error selecting and updating execution with executor "
          + executorId, e);
    }
  }

  /**
   * This method is used to select executions in batch. It will apply lock and fetch executions.
   * It will also update the status of those executions as mentioned in updatedStatus field.
   * @param batchEnabled If set to true, fetch the executions in batch
   * @param limit Limit in case of batch fetch
   * @param updatedStatus Update the status of executions as mentioned in this field. It can be
   *                      READY of PREPARING based on whichever is the starting state for any
   *                      dispatch method.
   * @return Set of execution ids
   * @throws ExecutorManagerException
   */
  public Set<Integer> selectAndUpdateExecutionWithLocking(final boolean batchEnabled,
      final int limit,
      final Status updatedStatus,
      final DispatchMethod dispatchMethod)
      throws ExecutorManagerException {
    final String UPDATE_EXECUTION = "UPDATE execution_flows SET status = ?, update_time = ? "
        + "where exec_id = ?";
    final SQLTransaction<Set<Integer>> selectAndUpdateExecution = transOperator -> {
      final Set<Integer> executions = new HashSet<>();
      final boolean hasLocked = this.mysqlNamedLock
          .getLock(transOperator, POLLING_LOCK_NAME, GET_LOCK_TIMEOUT_IN_SECONDS);
      logger.debug("ExecutionFlow polling lock value: " + hasLocked);
      if (hasLocked) {
        try {
          final List<Integer> execIds;
          if (batchEnabled) {
            execIds = transOperator.query(String
                    .format(SelectFromExecutionFlows.SELECT_EXECUTION_IN_BATCH_FOR_UPDATE_FORMAT, ""),
                new SelectFromExecutionFlows(), Status.READY.getNumVal(), dispatchMethod.getNumVal(), limit);
          } else {
            execIds = transOperator.query(
                String.format(SelectFromExecutionFlows.SELECT_EXECUTION_FOR_UPDATE_FORMAT, ""),
                new SelectFromExecutionFlows(), Status.READY.getNumVal(),
                dispatchMethod.getNumVal());
          }
          if (CollectionUtils.isNotEmpty(execIds)) {
            executions.addAll(execIds);
            //TODO: Currently transOperator.getConnection().createArrayOf is not supported so
            //update statement can not have {exec_id in (?)} in where clause. Use below
            //mentioned code instead of for look for update statement when createArrayOf is
            //supported
            //Array executionsToUpdate = transOperator.getConnection().createArrayOf("INTEGER",
            //    execIds.toArray());
            //transOperator
            //      .update(UPDATE_EXECUTION, updatedStatus.getNumVal(), System.currentTimeMillis(),
            //          executionsToUpdate);
            for (final Integer execId : execIds) {
              transOperator
                  .update(UPDATE_EXECUTION, updatedStatus.getNumVal(), System.currentTimeMillis(),
                      execId);
            }
          }
        } finally {
          this.mysqlNamedLock.releaseLock(transOperator, POLLING_LOCK_NAME);
          logger.debug("Released polling lock");
        }
      } else {
        logger.info("Could not acquire polling lock");
      }
      return executions;
    };

    try {
      return this.dbOperator.transaction(selectAndUpdateExecution);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error selecting and updating execution", e);
    }
  }

  /**
   * Updates version set id for the given executionId
   *
   * @param executionId
   * @param versionSetId
   * @return int
   * @throws ExecutorManagerException
   */
  public int updateVersionSetId(final int executionId, final int versionSetId)
      throws ExecutorManagerException {
    final String UPDATE_VERSION_SET_ID = "UPDATE execution_flows SET version_set_id = ?, "
        + "update_time = ? where exec_id = ?";
    try {
      return this.dbOperator.update(UPDATE_VERSION_SET_ID, versionSetId,
          System.currentTimeMillis(), executionId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(String.format("Error while updating version set id for "
          + "execId: %d", executionId), e);
    }
  }

  public static class SelectFromExecutionFlows implements
      ResultSetHandler<List<Integer>> {

    private static final String SELECT_EXECUTION_FOR_UPDATE_FORMAT =
        "SELECT exec_id from execution_flows WHERE exec_id = (SELECT exec_id from execution_flows"
            + " WHERE status = ? and dispatch_method = ?"
            + " and executor_id is NULL and flow_data is NOT NULL %s"
            + " ORDER BY flow_priority DESC, update_time ASC, exec_id ASC LIMIT 1) and "
            + "executor_id is NULL FOR UPDATE";

    private static final String SELECT_EXECUTION_IN_BATCH_FOR_UPDATE_FORMAT =
        "SELECT exec_id from execution_flows WHERE exec_id in (SELECT exec_id from execution_flows"
            + " WHERE status = ? and dispatch_method = ?"
            + " and executor_id is NULL and flow_data is NOT NULL %s ) "
            + " ORDER BY flow_priority DESC, update_time ASC, exec_id ASC "
            + " LIMIT ? FOR UPDATE";

    public static final String SELECT_EXECUTION_FOR_UPDATE_ACTIVE =
        String.format(SELECT_EXECUTION_FOR_UPDATE_FORMAT,
            "and (use_executor is NULL or use_executor = ?)");

    public static final String SELECT_EXECUTION_FOR_UPDATE_INACTIVE =
        String.format(SELECT_EXECUTION_FOR_UPDATE_FORMAT, "and use_executor = ?");

    @Override
    public List<Integer> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<Integer> execIds = new ArrayList<>();
      do {
        final int execId = rs.getInt(1);
        execIds.add(execId);
      } while (rs.next());

      return execIds;
    }
  }

  public static class FetchExecutableFlows implements
      ResultSetHandler<List<ExecutableFlow>> {

    static String FETCH_EXECUTABLE_FLOW_BY_START_TIME =
        "SELECT ef.exec_id, ef.enc_type, ef.flow_data, ef.status FROM execution_flows ef WHERE "
            + "project_id=? AND flow_id=? AND start_time >= ? ORDER BY start_time DESC";
    static String FETCH_BASE_EXECUTABLE_FLOW_QUERY =
        "SELECT ef.exec_id, ef.enc_type, ef.flow_data, ef.status FROM execution_flows ef";
    private static final String FETCH_FLOWS_STARTED_BEFORE = FETCH_BASE_EXECUTABLE_FLOW_QUERY +
        " WHERE start_time < ?";
    static String FETCH_EXECUTABLE_FLOW =
        "SELECT exec_id, enc_type, flow_data, status FROM execution_flows "
            + "WHERE exec_id=?";
    static String FETCH_ALL_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data, status FROM execution_flows "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    static String FETCH_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data, status FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    static String FETCH_EXECUTABLE_FLOW_BY_STATUS =
        "SELECT exec_id, enc_type, flow_data, status FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? AND status=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    // Fetch flows that are in preparing state for more than a certain duration.
    private static final String FETCH_FLOWS_QUEUED_FOR_LONG_TIME =
        "SELECT exec_id, enc_type, flow_data, status FROM execution_flows"
            + " WHERE submit_time < ? AND status = "
            + Status.PREPARING.getNumVal();

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
          final Status status = Status.fromInteger(rs.getInt(4));
          try {
            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlow(
                    GZIPUtils.transformBytesToObject(data, encType), status);
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
        "SELECT exec_id, enc_type, flow_data, status FROM execution_flows"
            + " WHERE executor_id is NULL AND status = ?";

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
          final Status status = Status.fromInteger(rs.getInt(4));
          try {
            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlow(
                    GZIPUtils.transformBytesToObject(data, encType), status);
            final ExecutionReference ref = new ExecutionReference(id, exFlow.getDispatchMethod());
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
        "SELECT exec_id, enc_type, flow_data, status FROM execution_flows "
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
          final Status status = Status.fromInteger(rs.getInt(4));
          try {
            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlow(
                    GZIPUtils.transformBytesToObject(data, encType), status);
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

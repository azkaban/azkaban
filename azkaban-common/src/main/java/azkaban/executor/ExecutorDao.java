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

import azkaban.database.AbstractJdbcLoader;
import azkaban.db.DatabaseOperator;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.Props;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

@Singleton
public class ExecutorDao extends AbstractJdbcLoader {

  private static final Logger logger = Logger.getLogger(ExecutorDao.class);
  private final DatabaseOperator dbOperator;

  @Inject
  public ExecutorDao(final Props props, final CommonMetrics commonMetrics,
                     final DatabaseOperator dbOperator) {
    super(props, commonMetrics);
    this.dbOperator = dbOperator;
  }

  public List<Executor> fetchAllExecutors() throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchExecutorHandler executorHandler = new FetchExecutorHandler();

    try {
      final List<Executor> executors =
          runner.query(FetchExecutorHandler.FETCH_ALL_EXECUTORS, executorHandler);
      return executors;
    } catch (final Exception e) {
      throw new ExecutorManagerException("Error fetching executors", e);
    }
  }

  public List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchExecutorHandler executorHandler = new FetchExecutorHandler();

    try {
      final List<Executor> executors =
          runner.query(FetchExecutorHandler.FETCH_ACTIVE_EXECUTORS,
              executorHandler);
      return executors;
    } catch (final Exception e) {
      throw new ExecutorManagerException("Error fetching active executors", e);
    }
  }

  public Executor fetchExecutor(final String host, final int port)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchExecutorHandler executorHandler = new FetchExecutorHandler();

    try {
      final List<Executor> executors =
          runner.query(FetchExecutorHandler.FETCH_EXECUTOR_BY_HOST_PORT,
              executorHandler, host, port);
      if (executors.isEmpty()) {
        return null;
      } else {
        return executors.get(0);
      }
    } catch (final Exception e) {
      throw new ExecutorManagerException(String.format(
          "Error fetching executor %s:%d", host, port), e);
    }
  }

  public Executor fetchExecutor(final int executorId) throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchExecutorHandler executorHandler = new FetchExecutorHandler();

    try {
      final List<Executor> executors =
          runner.query(FetchExecutorHandler.FETCH_EXECUTOR_BY_ID,
              executorHandler, executorId);
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

  public Executor fetchExecutorByExecutionId(final int executionId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchExecutorHandler executorHandler = new FetchExecutorHandler();
    Executor executor = null;
    try {
      final List<Executor> executors =
          runner.query(FetchExecutorHandler.FETCH_EXECUTION_EXECUTOR,
              executorHandler, executionId);
      if (executors.size() > 0) {
        executor = executors.get(0);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error fetching executor for exec_id : " + executionId, e);
    }
    return executor;
  }

  public Executor addExecutor(final String host, final int port)
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

  private void addExecutorHelper(final String host, final int port)
      throws ExecutorManagerException {
    final String INSERT = "INSERT INTO executors (host, port) values (?,?)";
    final QueryRunner runner = createQueryRunner();
    try {
      runner.update(INSERT, host, port);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(String.format("Error adding %s:%d ",
          host, port), e);
    }
  }

  public void removeExecutor(final String host, final int port) throws ExecutorManagerException {
    final String DELETE = "DELETE FROM executors WHERE host=? AND port=?";
    final QueryRunner runner = createQueryRunner();
    try {
      final int rows = runner.update(DELETE, host, port);
      if (rows == 0) {
        throw new ExecutorManagerException("No executor with host, port :"
            + "(" + host + "," + port + ")");
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error removing executor with host, port : "
          + "(" + host + "," + port + ")", e);
    }
  }

  /**
   * JDBC ResultSetHandler to fetch records from executors table
   */
  public static class FetchExecutorHandler implements
      ResultSetHandler<List<Executor>> {

    static String FETCH_ALL_EXECUTORS =
        "SELECT id, host, port, active FROM executors";
    static String FETCH_ACTIVE_EXECUTORS =
        "SELECT id, host, port, active FROM executors where active=true";
    static String FETCH_EXECUTOR_BY_ID =
        "SELECT id, host, port, active FROM executors where id=?";
    static String FETCH_EXECUTOR_BY_HOST_PORT =
        "SELECT id, host, port, active FROM executors where host=? AND port=?";
    static String FETCH_EXECUTION_EXECUTOR =
        "SELECT ex.id, ex.host, ex.port, ex.active FROM "
            + " executors ex INNER JOIN execution_flows ef "
            + "on ex.id = ef.executor_id  where exec_id=?";

    @Override
    public List<Executor> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final List<Executor> executors = new ArrayList<>();
      do {
        final int id = rs.getInt(1);
        final String host = rs.getString(2);
        final int port = rs.getInt(3);
        final boolean active = rs.getBoolean(4);
        final Executor executor = new Executor(id, host, port, active);
        executors.add(executor);
      } while (rs.next());

      return executors;
    }
  }
}

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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

@Singleton
public class ExecutorDao {

  private static final Logger logger = Logger.getLogger(ExecutorDao.class);
  private final DatabaseOperator dbOperator;

  @Inject
  public ExecutorDao(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  List<Executor> fetchAllExecutors() throws ExecutorManagerException {
    try {
      return this.dbOperator
          .query(FetchExecutorHandler.FETCH_ALL_EXECUTORS, new FetchExecutorHandler());
    } catch (final Exception e) {
      throw new ExecutorManagerException("Error fetching executors", e);
    }
  }

  List<Executor> fetchActiveExecutors() throws ExecutorManagerException {
    try {
      return this.dbOperator
          .query(FetchExecutorHandler.FETCH_ACTIVE_EXECUTORS, new FetchExecutorHandler());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active executors", e);
    }
  }

  public Executor fetchExecutor(final String host, final int port)
      throws ExecutorManagerException {
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

  Executor fetchExecutorByExecutionId(final int executionId)
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

  Executor addExecutor(final String host, final int port, final ExecutorTags tags)
      throws ExecutorManagerException {
    // verify, if executor already exists
    if (fetchExecutor(host, port) != null) {
      throw new ExecutorManagerException(String.format(
          "Executor %s:%d already exist", host, port));
    }
    // add new executor
    addExecutorHelper(host, port, tags);

    // fetch newly added executor
    return fetchExecutor(host, port);
  }

  private void addExecutorHelper(final String host, final int port, final ExecutorTags tags)
      throws ExecutorManagerException {
    final String INSERT = "INSERT INTO executors (host, port) values (?,?)";
    final String INSERT_TAG = "INSERT INTO executor_tags (executor_id, tag) VALUES (?,?)";
    try {
      this.dbOperator.transaction(transOperator -> {
        transOperator.update(INSERT, host, port);
        final long executorId = transOperator.getLastInsertId();
        for (final String tag : tags) {
          transOperator.update(INSERT_TAG, executorId, tag);
        }
        return executorId;
      });
    } catch (final SQLException e) {
      throw new ExecutorManagerException(String.format("Error adding %s:%d ",
          host, port), e);
    }
  }

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

  void removeExecutor(final String host, final int port) throws ExecutorManagerException {
    final String DELETE = "DELETE FROM executors WHERE host=? AND port=?";
    final String DELETE_TAGS = "DELETE FROM executor_tags"
        + " WHERE executor_id = (SELECT id FROM executors WHERE host=? AND port=?)";
    try {
      final int rows = this.dbOperator.transaction(transOperator -> {
        transOperator.update(DELETE_TAGS, host, port);
        return transOperator.update(DELETE, host, port);
      });
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

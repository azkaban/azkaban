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
import azkaban.executor.ExecutorLogEvent.EventType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;

@Singleton
public class ExecutorEventsDao {

  private final DatabaseOperator dbOperator;

  @Inject
  public ExecutorEventsDao(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  public void postExecutorEvent(final Executor executor, final EventType type, final String user,
      final String message) throws ExecutorManagerException {
    final String INSERT_PROJECT_EVENTS =
        "INSERT INTO executor_events (executor_id, event_type, event_time, username, message) values (?,?,?,?,?)";
    try {
      this.dbOperator.update(INSERT_PROJECT_EVENTS, executor.getId(), type.getNumVal(),
          new Date(), user, message);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Failed to post executor event", e);
    }
  }

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
        return Collections.<ExecutorLogEvent>emptyList();
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

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

  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(FetchActiveExecutableFlows.FETCH_ACTIVE_EXECUTABLE_FLOW,
          new FetchActiveExecutableFlows());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  @VisibleForTesting
  static class FetchActiveExecutableFlows implements
      ResultSetHandler<Map<Integer, Pair<ExecutionReference, ExecutableFlow>>> {

    // Select running and executor assigned flows
    private static final String FETCH_ACTIVE_EXECUTABLE_FLOW =
        "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
            + "et.port port, ex.executor_id executorId, et.active executorStatus"
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
        final int id = rs.getInt(1);
        final int encodingType = rs.getInt(2);
        final byte[] data = rs.getBytes(3);
        final String host = rs.getString(4);
        final int port = rs.getInt(5);
        final int executorId = rs.getInt(6);
        final boolean executorStatus = rs.getBoolean(7);

        if (data == null) {
          logger.warn("Execution id " + id + " has flow_data=null. To clean up, update status to "
              + "FAILED manually, eg. "
              + "SET status = " + Status.FAILED.getNumVal() + " WHERE id = " + id);
        } else {
          final EncodingType encType = EncodingType.fromInteger(encodingType);
          try {
            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(
                    GZIPUtils.transformBytesToObject(data, encType));
            final Executor executor;
            if (host == null) {
              logger.warn("Executor id " + executorId + " (on execution " + id + ") wasn't found");
              executor = null;
            } else {
              executor = new Executor(executorId, host, port, executorStatus);
            }
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

}

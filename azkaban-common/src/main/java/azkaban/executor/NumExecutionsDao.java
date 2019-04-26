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
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;


@Singleton
public class NumExecutionsDao {

  private final DatabaseOperator dbOperator;

  @Inject
  public NumExecutionsDao(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  public int fetchNumExecutableFlows() throws ExecutorManagerException {
    try {
      return this.dbOperator.query(IntHandler.NUM_EXECUTIONS, new IntHandler());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  public int fetchNumExecutableFlows(final int projectId, final String flowId)
      throws ExecutorManagerException {
    final IntHandler intHandler = new IntHandler();
    try {
      return this.dbOperator.query(IntHandler.NUM_FLOW_EXECUTIONS, intHandler, projectId, flowId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  public int fetchNumExecutableNodes(final int projectId, final String jobId)
      throws ExecutorManagerException {
    final IntHandler intHandler = new IntHandler();
    try {
      return this.dbOperator.query(IntHandler.NUM_JOB_EXECUTIONS, intHandler, projectId, jobId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching num executions", e);
    }
  }

  private static class IntHandler implements ResultSetHandler<Integer> {

    private static final String NUM_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_flows";
    private static final String NUM_FLOW_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_flows WHERE project_id=? AND flow_id=?";
    private static final String NUM_JOB_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_jobs WHERE project_id=? AND job_id=?";

    @Override
    public Integer handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }
}

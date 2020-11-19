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
import java.sql.SQLException;
import javax.inject.Inject;
import javax.inject.Singleton;

//TODO jamiesjc: This class is deprecated as we don't fetch active_execution_flow table any longer.
// So this class should be removed onwards.
@Deprecated
@Singleton
public class ActiveExecutingFlowsDao {

  private final DatabaseOperator dbOperator;

  @Inject
  public ActiveExecutingFlowsDao(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  void addActiveExecutableReference(final ExecutionReference reference)
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

  void removeActiveExecutableReference(final int execId)
      throws ExecutorManagerException {
    final String DELETE = "DELETE FROM active_executing_flows WHERE exec_id=?";
    try {
      this.dbOperator.update(DELETE, execId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execId, e);
    }
  }

  boolean updateExecutableReference(final int execId, final long updateTime)
      throws ExecutorManagerException {
    final String DELETE =
        "UPDATE active_executing_flows set update_time=? WHERE exec_id=?";

    try {
      // Should be 1.
      return this.dbOperator.update(DELETE, updateTime, execId) > 0;
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execId, e);
    }
  }
}

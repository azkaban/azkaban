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

@Singleton
public class AssignExecutorDao {

  private final ExecutorDao executorDao;
  private final DatabaseOperator dbOperator;

  @Inject
  public AssignExecutorDao(final DatabaseOperator dbOperator,
      final ExecutorDao executorDao) {
    this.dbOperator = dbOperator;
    this.executorDao = executorDao;
  }

  public void assignExecutor(final int executorId, final int executionId)
      throws ExecutorManagerException {
    final String UPDATE =
        "UPDATE execution_flows SET executor_id=? where exec_id=?";
    try {
      if (this.executorDao.fetchExecutor(executorId) == null) {
        throw new ExecutorManagerException(String.format(
            "Failed to assign non-existent executor Id: %d to execution : %d  ",
            executorId, executionId));
      }

      if (this.dbOperator.update(UPDATE, executorId, executionId) == 0) {
        throw new ExecutorManagerException(String.format(
            "Failed to assign executor Id: %d to non-existent execution : %d  ",
            executorId, executionId));
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating executor id "
          + executorId, e);
    }
  }

  void unassignExecutor(final int executionId) throws ExecutorManagerException {
    final String UPDATE = "UPDATE execution_flows SET executor_id=NULL where exec_id=?";
    try {
      final int rows = this.dbOperator.update(UPDATE, executionId);
      if (rows == 0) {
        throw new ExecutorManagerException(String.format(
            "Failed to unassign executor for execution : %d  ", executionId));
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating execution id " + executionId, e);
    }
  }
}

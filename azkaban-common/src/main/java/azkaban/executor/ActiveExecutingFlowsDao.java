package azkaban.executor;

import azkaban.database.AbstractJdbcLoader;
import azkaban.db.DatabaseOperator;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.Props;
import java.sql.SQLException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.QueryRunner;

@Singleton
class ActiveExecutingFlowsDao extends AbstractJdbcLoader{

  private final DatabaseOperator dbOperator;

  @Inject
  ActiveExecutingFlowsDao(final Props props, final CommonMetrics commonMetrics,
                          final DatabaseOperator dbOperator) {
    super(props, commonMetrics);
    this.dbOperator = dbOperator;
  }

  void addActiveExecutableReference(final ExecutionReference reference)
      throws ExecutorManagerException {
    final String INSERT =
        "INSERT INTO active_executing_flows "
            + "(exec_id, update_time) values (?,?)";
    final QueryRunner runner = createQueryRunner();

    try {
      runner.update(INSERT, reference.getExecId(), reference.getUpdateTime());
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error updating active flow reference " + reference.getExecId(), e);
    }
  }

  void removeActiveExecutableReference(final int execid)
      throws ExecutorManagerException {
    final String DELETE = "DELETE FROM active_executing_flows WHERE exec_id=?";

    final QueryRunner runner = createQueryRunner();
    try {
      runner.update(DELETE, execid);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execid, e);
    }
  }

  boolean updateExecutableReference(final int execId, final long updateTime)
      throws ExecutorManagerException {
    final String DELETE =
        "UPDATE active_executing_flows set update_time=? WHERE exec_id=?";

    final QueryRunner runner = createQueryRunner();
    int updateNum = 0;
    try {
      updateNum = runner.update(DELETE, updateTime, execId);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error deleting active flow reference " + execId, e);
    }

    // Should be 1.
    return updateNum > 0;
  }
}
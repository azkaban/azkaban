package azkaban.executor;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;

public class IntHandler implements ResultSetHandler<Integer> {
  public static final String NUM_EXECUTIONS =
      "SELECT COUNT(1) FROM execution_flows";
  public static final String NUM_FLOW_EXECUTIONS =
      "SELECT COUNT(1) FROM execution_flows WHERE project_id=? AND flow_id=?";
  public static final String NUM_JOB_EXECUTIONS =
      "SELECT COUNT(1) FROM execution_jobs WHERE project_id=? AND job_id=?";
  public static final String FETCH_EXECUTION_LOGS_COUNT = "SELECT COUNT(1) execution_logs WHERE "
      + "upload_time < ?";

  @Override
  public Integer handle(final ResultSet rs) throws SQLException {
    if (!rs.next()) {
      return 0;
    }
    return rs.getInt(1);
  }
}

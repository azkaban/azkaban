package azkaban.executor;

import azkaban.database.AbstractJdbcLoader;
import azkaban.db.DatabaseOperator;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

@Singleton
public class FetchActiveFlowDao extends AbstractJdbcLoader {

  private static final Logger logger = Logger.getLogger(FetchActiveFlowDao.class);
  private final DatabaseOperator dbOperator;

  @Inject
  public FetchActiveFlowDao(final Props props, final CommonMetrics commonMetrics,
                            final DatabaseOperator dbOperator) {
    super(props, commonMetrics);
    this.dbOperator = dbOperator;
  }

  Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows()
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchActiveExecutableFlows flowHandler = new FetchActiveExecutableFlows();

    try {
      final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> properties =
          runner.query(FetchActiveExecutableFlows.FETCH_ACTIVE_EXECUTABLE_FLOW,
              flowHandler);
      return properties;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows", e);
    }
  }

  Pair<ExecutionReference, ExecutableFlow> fetchActiveFlowByExecId(final int execId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    final FetchActiveExecutableFlowByExecId flowHandler = new FetchActiveExecutableFlowByExecId();

    try {
      final List<Pair<ExecutionReference, ExecutableFlow>> flows =
          runner.query(
              FetchActiveExecutableFlowByExecId.FETCH_ACTIVE_EXECUTABLE_FLOW_BY_EXECID,
              flowHandler, execId);
      if (flows.isEmpty()) {
        return null;
      } else {
        return flows.get(0);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active flows by exec id", e);
    }
  }

  private static class FetchActiveExecutableFlows implements
      ResultSetHandler<Map<Integer, Pair<ExecutionReference, ExecutableFlow>>> {

    // Select running and executor assigned flows
    private static final String FETCH_ACTIVE_EXECUTABLE_FLOW =
        "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
            + "et.port port, et.id executorId, et.active executorStatus"
            + " FROM execution_flows ex"
            + " INNER JOIN "
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
          execFlows.put(id, null);
        } else {
          final EncodingType encType = EncodingType.fromInteger(encodingType);
          final Object flowObj;
          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              final String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              final String jsonString = new String(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            }

            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(flowObj);
            final Executor executor = new Executor(executorId, host, port, executorStatus);
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

  private static class FetchActiveExecutableFlowByExecId implements
      ResultSetHandler<List<Pair<ExecutionReference, ExecutableFlow>>> {

    private static final String FETCH_ACTIVE_EXECUTABLE_FLOW_BY_EXECID =
        "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data, et.host host, "
            + "et.port port, et.id executorId, et.active executorStatus"
            + " FROM execution_flows ex"
            + " INNER JOIN "
            + " executors et ON ex.executor_id = et.id"
            + " Where ex.exec_id = ? AND ex.status NOT IN ("
            + Status.SUCCEEDED.getNumVal() + ", "
            + Status.KILLED.getNumVal() + ", "
            + Status.FAILED.getNumVal() + ")";

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
        final String host = rs.getString(4);
        final int port = rs.getInt(5);
        final int executorId = rs.getInt(6);
        final boolean executorStatus = rs.getBoolean(7);

        if (data == null) {
          logger.error("Found a flow with empty data blob exec_id: " + id);
        } else {
          final EncodingType encType = EncodingType.fromInteger(encodingType);
          final Object flowObj;
          try {
            if (encType == EncodingType.GZIP) {
              final String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              final String jsonString = new String(data, "UTF-8");
              flowObj = JSONUtils.parseJSONFromString(jsonString);
            }

            final ExecutableFlow exFlow =
                ExecutableFlow.createExecutableFlowFromObject(flowObj);
            final Executor executor = new Executor(executorId, host, port, executorStatus);
            final ExecutionReference ref = new ExecutionReference(id, executor);
            execFlows.add(new Pair<>(ref, exFlow));
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }
}
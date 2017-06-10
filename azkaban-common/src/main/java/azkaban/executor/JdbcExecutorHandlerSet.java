package azkaban.executor;

import azkaban.database.AbstractJdbcLoader.EncodingType;
import azkaban.executor.ExecutorLogEvent.EventType;
import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

public class JdbcExecutorHandlerSet {

  private static final Logger logger = Logger.getLogger(JdbcExecutorImpl.class);

  public static class FetchExecutableFlows implements
      ResultSetHandler<List<ExecutableFlow>> {

    public static String FETCH_BASE_EXECUTABLE_FLOW_QUERY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows ";
    public static String FETCH_EXECUTABLE_FLOW =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE exec_id=?";
    // private static String FETCH_ACTIVE_EXECUTABLE_FLOW =
    // "SELECT ex.exec_id exec_id, ex.enc_type enc_type, ex.flow_data flow_data "
    // +
    // "FROM execution_flows ex " +
    // "INNER JOIN active_executing_flows ax ON ex.exec_id = ax.exec_id";
    public static String FETCH_ALL_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    public static String FETCH_EXECUTABLE_FLOW_HISTORY =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";
    public static String FETCH_EXECUTABLE_FLOW_BY_STATUS =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE project_id=? AND flow_id=? AND status=? "
            + "ORDER BY exec_id DESC LIMIT ?, ?";

    @Override
    public List<ExecutableFlow> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutableFlow> emptyList();
      }

      final List<ExecutableFlow> execFlows = new ArrayList<>();
      do {
        final int id = rs.getInt(1);
        final int encodingType = rs.getInt(2);
        final byte[] data = rs.getBytes(3);

        if (data != null) {
          final EncodingType encType = EncodingType.fromInteger(encodingType);
          final Object flowObj;
          try {
            // Convoluted way to inflate strings. Should find common package
            // or helper function.
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
            execFlows.add(exFlow);
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }

  /**
   * JDBC ResultSetHandler to fetch records from executors table
   */
  public static class FetchExecutorHandler implements
      ResultSetHandler<List<Executor>> {

    public static String FETCH_ALL_EXECUTORS =
        "SELECT id, host, port, active FROM executors";
    public static String FETCH_ACTIVE_EXECUTORS =
        "SELECT id, host, port, active FROM executors where active=true";
    public static String FETCH_EXECUTOR_BY_ID =
        "SELECT id, host, port, active FROM executors where id=?";
    public static String FETCH_EXECUTOR_BY_HOST_PORT =
        "SELECT id, host, port, active FROM executors where host=? AND port=?";
    public static String FETCH_EXECUTION_EXECUTOR =
        "SELECT ex.id, ex.host, ex.port, ex.active FROM "
            + " executors ex INNER JOIN execution_flows ef "
            + "on ex.id = ef.executor_id  where exec_id=?";

    @Override
    public List<Executor> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Executor> emptyList();
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

  /**
   * JDBC ResultSetHandler to fetch records from executor_events table
   */
  public static class ExecutorLogsResultHandler implements
      ResultSetHandler<List<ExecutorLogEvent>> {

    public static String SELECT_EXECUTOR_EVENTS_ORDER =
        "SELECT executor_id, event_type, event_time, username, message FROM executor_events "
            + " WHERE executor_id=? ORDER BY event_time LIMIT ? OFFSET ?";

    @Override
    public List<ExecutorLogEvent> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutorLogEvent> emptyList();
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

  public static class FetchLogsHandler implements ResultSetHandler<LogData> {

    public static String FETCH_LOGS =
        "SELECT exec_id, name, attempt, enc_type, start_byte, end_byte, log "
            + "FROM execution_logs "
            + "WHERE exec_id=? AND name=? AND attempt=? AND end_byte > ? "
            + "AND start_byte <= ? ORDER BY start_byte";

    private final int startByte;
    private final int endByte;

    public FetchLogsHandler(final int startByte, final int endByte) {
      this.startByte = startByte;
      this.endByte = endByte;
    }

    @Override
    public LogData handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }

      final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      do {
        // int execId = rs.getInt(1);
        // String name = rs.getString(2);
        final int attempt = rs.getInt(3);
        final EncodingType encType = EncodingType.fromInteger(rs.getInt(4));
        final int startByte = rs.getInt(5);
        final int endByte = rs.getInt(6);

        final byte[] data = rs.getBytes(7);

        final int offset =
            this.startByte > startByte ? this.startByte - startByte : 0;
        final int length =
            this.endByte < endByte ? this.endByte - startByte - offset
                : endByte - startByte - offset;
        try {
          byte[] buffer = data;
          if (encType == EncodingType.GZIP) {
            buffer = GZIPUtils.unGzipBytes(data);
          }

          byteStream.write(buffer, offset, length);
        } catch (final IOException e) {
          throw new SQLException(e);
        }
      } while (rs.next());

      final byte[] buffer = byteStream.toByteArray();
      final Pair<Integer, Integer> result =
          FileIOUtils.getUtf8Range(buffer, 0, buffer.length);

      return new LogData(this.startByte + result.getFirst(), result.getSecond(),
          new String(buffer, result.getFirst(), result.getSecond(), StandardCharsets.UTF_8));
    }
  }

  public static class FetchExecutableJobHandler implements
      ResultSetHandler<List<ExecutableJobInfo>> {

    public static String FETCH_EXECUTABLE_NODE =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt "
            + "FROM execution_jobs WHERE exec_id=? "
            + "AND job_id=? AND attempt=?";
    public static String FETCH_EXECUTABLE_NODE_ATTEMPTS =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt FROM execution_jobs "
            + "WHERE exec_id=? AND job_id=?";
    public static String FETCH_PROJECT_EXECUTABLE_NODE =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt FROM execution_jobs "
            + "WHERE project_id=? AND job_id=? "
            + "ORDER BY exec_id DESC LIMIT ?, ? ";

    @Override
    public List<ExecutableJobInfo> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ExecutableJobInfo> emptyList();
      }

      final List<ExecutableJobInfo> execNodes = new ArrayList<>();
      do {
        final int execId = rs.getInt(1);
        final int projectId = rs.getInt(2);
        final int version = rs.getInt(3);
        final String flowId = rs.getString(4);
        final String jobId = rs.getString(5);
        final long startTime = rs.getLong(6);
        final long endTime = rs.getLong(7);
        final Status status = Status.fromInteger(rs.getInt(8));
        final int attempt = rs.getInt(9);

        final ExecutableJobInfo info =
            new ExecutableJobInfo(execId, projectId, version, flowId, jobId,
                startTime, endTime, status, attempt);
        execNodes.add(info);
      } while (rs.next());

      return execNodes;
    }
  }

  public static class FetchExecutableJobAttachmentsHandler implements
      ResultSetHandler<String> {

    public static String FETCH_ATTACHMENTS_EXECUTABLE_NODE =
        "SELECT attachments FROM execution_jobs WHERE exec_id=? AND job_id=?";

    @Override
    public String handle(final ResultSet rs) throws SQLException {
      String attachmentsJson = null;
      if (rs.next()) {
        try {
          final byte[] attachments = rs.getBytes(1);
          if (attachments != null) {
            attachmentsJson = GZIPUtils.unGzipString(attachments, "UTF-8");
          }
        } catch (final IOException e) {
          throw new SQLException("Error decoding job attachments", e);
        }
      }
      return attachmentsJson;
    }
  }

    public static class FetchExecutableJobPropsHandler implements
      ResultSetHandler<Pair<Props, Props>> {

    public static String FETCH_OUTPUT_PARAM_EXECUTABLE_NODE =
        "SELECT output_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
    public static String FETCH_INPUT_PARAM_EXECUTABLE_NODE =
        "SELECT input_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
    public static String FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE =
        "SELECT input_params, output_params "
            + "FROM execution_jobs WHERE exec_id=? AND job_id=?";

    @Override
    public Pair<Props, Props> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return new Pair<>(null, null);
      }

      if (rs.getMetaData().getColumnCount() > 1) {
        final byte[] input = rs.getBytes(1);
        final byte[] output = rs.getBytes(2);

        Props inputProps = null;
        Props outputProps = null;
        try {
          if (input != null) {
            final String jsonInputString = GZIPUtils.unGzipString(input, "UTF-8");
            inputProps =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonInputString));

          }
          if (output != null) {
            final String jsonOutputString = GZIPUtils.unGzipString(output, "UTF-8");
            outputProps =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonOutputString));
          }
        } catch (final IOException e) {
          throw new SQLException("Error decoding param data", e);
        }

        return new Pair<>(inputProps, outputProps);
      } else {
        final byte[] params = rs.getBytes(1);
        Props props = null;
        try {
          if (params != null) {
            final String jsonProps = GZIPUtils.unGzipString(params, "UTF-8");

            props =
                PropsUtils.fromHierarchicalMap((Map<String, Object>) JSONUtils
                    .parseJSONFromString(jsonProps));
          }
        } catch (final IOException e) {
          throw new SQLException("Error decoding param data", e);
        }

        return new Pair<>(props, null);
      }
    }
  }

  /**
   * JDBC ResultSetHandler to fetch queued executions
   */
  public static class FetchQueuedExecutableFlows implements
      ResultSetHandler<List<Pair<ExecutionReference, ExecutableFlow>>> {
    // Select queued unassigned flows
    public static final String FETCH_QUEUED_EXECUTABLE_FLOW =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows"
            + " Where executor_id is NULL AND status = "
            + Status.PREPARING.getNumVal();

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

        if (data == null) {
          logger.error("Found a flow with empty data blob exec_id: " + id);
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
            final ExecutionReference ref = new ExecutionReference(id);
            execFlows.add(new Pair<>(ref, exFlow));
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }

  public static class FetchRecentlyFinishedFlows implements
      ResultSetHandler<List<ExecutableFlow>> {
    // Execution_flows table is already indexed by end_time
    public static String FETCH_RECENTLY_FINISHED_FLOW =
        "SELECT exec_id, enc_type, flow_data FROM execution_flows "
            + "WHERE end_time > ? AND status IN (?, ?, ?)";

    @Override
    public List<ExecutableFlow> handle(
        final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final List<ExecutableFlow> execFlows = new ArrayList<>();
      do {
        final int id = rs.getInt(1);
        final int encodingType = rs.getInt(2);
        final byte[] data = rs.getBytes(3);

        if (data != null) {
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

            execFlows.add(exFlow);
          } catch (final IOException e) {
            throw new SQLException("Error retrieving flow data " + id, e);
          }
        }
      } while (rs.next());

      return execFlows;
    }
  }

  public static class FetchActiveExecutableFlows implements
      ResultSetHandler<Map<Integer, Pair<ExecutionReference, ExecutableFlow>>> {
    // Select running and executor assigned flows
    public static String FETCH_ACTIVE_EXECUTABLE_FLOW =
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

  public static class FetchActiveExecutableFlowByExecId implements
      ResultSetHandler<List<Pair<ExecutionReference, ExecutableFlow>>> {

    public static String FETCH_ACTIVE_EXECUTABLE_FLOW_BY_EXECID =
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

  public static class IntHandler implements ResultSetHandler<Integer> {

    public static String NUM_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_flows";
    public static String NUM_FLOW_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_flows WHERE project_id=? AND flow_id=?";
    public static String NUM_JOB_EXECUTIONS =
        "SELECT COUNT(1) FROM execution_jobs WHERE project_id=? AND job_id=?";
    public static String FETCH_EXECUTOR_ID =
        "SELECT executor_id FROM execution_flows WHERE exec_id=?";

    @Override
    public Integer handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }
}

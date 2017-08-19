package azkaban.executor;

import azkaban.database.AbstractJdbcLoader;
import azkaban.db.DatabaseOperator;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

@Singleton
class ExecutionJobDao extends AbstractJdbcLoader{

  private static final Logger logger = Logger.getLogger(ExecutorDao.class);
  private final DatabaseOperator dbOperator;
  private final EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  ExecutionJobDao(final Props props, final CommonMetrics commonMetrics,
                  final DatabaseOperator databaseOperator) {
    super(props, commonMetrics);
    this.dbOperator = databaseOperator;
  }

  void uploadExecutableNode(final ExecutableNode node, final Props inputProps)
      throws ExecutorManagerException {
    final String INSERT_EXECUTION_NODE =
        "INSERT INTO execution_jobs "
            + "(exec_id, project_id, version, flow_id, job_id, start_time, "
            + "end_time, status, input_params, attempt) VALUES (?,?,?,?,?,?,?,?,?,?)";

    byte[] inputParam = null;
    if (inputProps != null) {
      try {
        final String jsonString =
            JSONUtils.toJSON(PropsUtils.toHierarchicalMap(inputProps));
        inputParam = GZIPUtils.gzipString(jsonString, "UTF-8");
      } catch (final IOException e) {
        throw new ExecutorManagerException("Error encoding input params");
      }
    }

    final ExecutableFlow flow = node.getExecutableFlow();
    final String flowId = node.getParentFlow().getFlowPath();
    System.out.println("Uploading flowId " + flowId);
    final QueryRunner runner = createQueryRunner();
    try {
      runner.update(INSERT_EXECUTION_NODE, flow.getExecutionId(),
          flow.getProjectId(), flow.getVersion(), flowId, node.getId(),
          node.getStartTime(), node.getEndTime(), node.getStatus().getNumVal(),
          inputParam, node.getAttempt());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error writing job " + node.getId(), e);
    }
  }

  void updateExecutableNode(final ExecutableNode node)
      throws ExecutorManagerException {
    final String UPSERT_EXECUTION_NODE =
        "UPDATE execution_jobs "
            + "SET start_time=?, end_time=?, status=?, output_params=? "
            + "WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";

    byte[] outputParam = null;
    final Props outputProps = node.getOutputProps();
    if (outputProps != null) {
      try {
        final String jsonString =
            JSONUtils.toJSON(PropsUtils.toHierarchicalMap(outputProps));
        outputParam = GZIPUtils.gzipString(jsonString, "UTF-8");
      } catch (final IOException e) {
        throw new ExecutorManagerException("Error encoding input params");
      }
    }

    final QueryRunner runner = createQueryRunner();
    try {
      runner.update(UPSERT_EXECUTION_NODE, node.getStartTime(), node
          .getEndTime(), node.getStatus().getNumVal(), outputParam, node
          .getExecutableFlow().getExecutionId(), node.getParentFlow()
          .getFlowPath(), node.getId(), node.getAttempt());
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error updating job " + node.getId(),
          e);
    }
  }

  List<ExecutableJobInfo> fetchJobInfoAttempts(final int execId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    try {
      final List<ExecutableJobInfo> info =
          runner.query(
              FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE_ATTEMPTS,
              new FetchExecutableJobHandler(), execId, jobId);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  ExecutableJobInfo fetchJobInfo(final int execId, final String jobId, final int attempts)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    try {
      final List<ExecutableJobInfo> info =
          runner.query(FetchExecutableJobHandler.FETCH_EXECUTABLE_NODE,
              new FetchExecutableJobHandler(), execId, jobId, attempts);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info.get(0);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  Props fetchExecutionJobInputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    try {
      final Pair<Props, Props> props =
          runner.query(
              FetchExecutableJobPropsHandler.FETCH_INPUT_PARAM_EXECUTABLE_NODE,
              new FetchExecutableJobPropsHandler(), execId, jobId);
      return props.getFirst();
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  Props fetchExecutionJobOutputProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    try {
      final Pair<Props, Props> props =
          runner
              .query(
                  FetchExecutableJobPropsHandler.FETCH_OUTPUT_PARAM_EXECUTABLE_NODE,
                  new FetchExecutableJobPropsHandler(), execId, jobId);
      return props.getFirst();
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  Pair<Props, Props> fetchExecutionJobProps(final int execId, final String jobId)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();
    try {
      final Pair<Props, Props> props = runner .query(
                  FetchExecutableJobPropsHandler.FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE,
                  new FetchExecutableJobPropsHandler(), execId, jobId);
      return props;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job params " + execId
          + " " + jobId, e);
    }
  }

  List<ExecutableJobInfo> fetchJobHistory(final int projectId, final String jobId,
                                          final int skip, final int size)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    try {
      final List<ExecutableJobInfo> info =
          runner.query(FetchExecutableJobHandler.FETCH_PROJECT_EXECUTABLE_NODE,
              new FetchExecutableJobHandler(), projectId, jobId, skip, size);
      if (info == null || info.isEmpty()) {
        return null;
      }
      return info;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error querying job info " + jobId, e);
    }
  }

  List<Object> fetchAttachments(final int execId, final String jobId, final int attempt)
      throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    try {
      final String attachments =
          runner
              .query(
                  FetchExecutableJobAttachmentsHandler.FETCH_ATTACHMENTS_EXECUTABLE_NODE,
                  new FetchExecutableJobAttachmentsHandler(), execId, jobId);
      if (attachments == null) {
        return null;
      }

      final List<Object> attachmentList =
          (List<Object>) JSONUtils.parseJSONFromString(attachments);

      return attachmentList;
    } catch (final IOException e) {
      throw new ExecutorManagerException(
          "Error converting job attachments to JSON " + jobId, e);
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          "Error query job attachments " + jobId, e);
    }
  }

  void uploadAttachmentFile(final ExecutableNode node, final File file)
      throws  ExecutorManagerException {
    final Connection connection = getConnection();
    try {
      uploadAttachmentFile(connection, node, file, this.defaultEncodingType);
      connection.commit();
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error committing attachments ", e);
    } catch (final IOException e) {
      throw new ExecutorManagerException("Error uploading attachments ", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void uploadAttachmentFile(final Connection connection, final ExecutableNode node,
                                    final File file, final EncodingType encType) throws SQLException, IOException {

    final String jsonString = FileUtils.readFileToString(file);
    final byte[] attachments = GZIPUtils.gzipString(jsonString, "UTF-8");

    final String UPDATE_EXECUTION_NODE_ATTACHMENTS =
        "UPDATE execution_jobs " + "SET attachments=? "
            + "WHERE exec_id=? AND flow_id=? AND job_id=? AND attempt=?";

    final QueryRunner runner = new QueryRunner();
    runner.update(connection, UPDATE_EXECUTION_NODE_ATTACHMENTS, attachments,
        node.getExecutableFlow().getExecutionId(), node.getParentFlow()
            .getNestedId(), node.getId(), node.getAttempt());
  }

  /**
   * TODO kunkun-tang: Will be removed during the next refactor.
   */
  private Connection getConnection() throws ExecutorManagerException {
    Connection connection = null;
    try {
      connection = super.getDBConnection(false);
    } catch (final Exception e) {
      DbUtils.closeQuietly(connection);
      throw new ExecutorManagerException("Error getting DB connection.", e);
    }
    return connection;
  }

  private static class FetchExecutableJobHandler implements
      ResultSetHandler<List<ExecutableJobInfo>> {
    private static final String FETCH_EXECUTABLE_NODE =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt "
            + "FROM execution_jobs WHERE exec_id=? "
            + "AND job_id=? AND attempt=?";
    private static final String FETCH_EXECUTABLE_NODE_ATTEMPTS =
        "SELECT exec_id, project_id, version, flow_id, job_id, "
            + "start_time, end_time, status, attempt FROM execution_jobs "
            + "WHERE exec_id=? AND job_id=?";
    private static final String FETCH_PROJECT_EXECUTABLE_NODE =
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

  private static class FetchExecutableJobPropsHandler implements
      ResultSetHandler<Pair<Props, Props>> {
    private static final String FETCH_OUTPUT_PARAM_EXECUTABLE_NODE =
        "SELECT output_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
    private static final String FETCH_INPUT_PARAM_EXECUTABLE_NODE =
        "SELECT input_params FROM execution_jobs WHERE exec_id=? AND job_id=?";
    private static final String FETCH_INPUT_OUTPUT_PARAM_EXECUTABLE_NODE =
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

  private static class FetchExecutableJobAttachmentsHandler implements
      ResultSetHandler<String> {
    private static final String FETCH_ATTACHMENTS_EXECUTABLE_NODE =
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
}
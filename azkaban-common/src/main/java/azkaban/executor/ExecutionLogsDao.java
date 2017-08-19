package azkaban.executor;

import azkaban.database.AbstractJdbcLoader;
import azkaban.db.DatabaseOperator;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.GZIPUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;

@Singleton
public class ExecutionLogsDao extends AbstractJdbcLoader{

  private final DatabaseOperator dbOperator;
  private final EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  ExecutionLogsDao(final Props props, final CommonMetrics commonMetrics,
                   final DatabaseOperator dbOperator) {
    super(props, commonMetrics);
    this.dbOperator = dbOperator;
  }

  LogData fetchLogs(final int execId, final String name, final int attempt,
                    final int startByte,
                    final int length) throws ExecutorManagerException {
    final QueryRunner runner = createQueryRunner();

    final FetchLogsHandler handler = new FetchLogsHandler(startByte, length + startByte);
    try {
      final LogData result =
          runner.query(FetchLogsHandler.FETCH_LOGS, handler, execId, name,
              attempt, startByte, startByte + length);
      return result;
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching logs " + execId
          + " : " + name, e);
    }
  }

  void uploadLogFile(final int execId, final String name, final int attempt,
                     final File... files)
      throws ExecutorManagerException {
    final Connection connection = getConnection();
    try {
      uploadLogFile(connection, execId, name, attempt, files,
          getDefaultEncodingType());
      connection.commit();
    } catch (final SQLException | IOException e) {
      throw new ExecutorManagerException("Error committing log", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  void uploadLogFile(final Connection connection, final int execId, final String name,
                     final int attempt, final File[] files, final EncodingType encType)
      throws ExecutorManagerException, IOException {
    // 50K buffer... if logs are greater than this, we chunk.
    // However, we better prevent large log files from being uploaded somehow
    final byte[] buffer = new byte[50 * 1024];
    int pos = 0;
    int length = buffer.length;
    int startByte = 0;
    try {
      for (int i = 0; i < files.length; ++i) {
        final File file = files[i];

        final BufferedInputStream bufferedStream =
            new BufferedInputStream(new FileInputStream(file));
        try {
          int size = bufferedStream.read(buffer, pos, length);
          while (size >= 0) {
            if (pos + size == buffer.length) {
              // Flush here.
              uploadLogPart(connection, execId, name, attempt, startByte,
                  startByte + buffer.length, encType, buffer, buffer.length);

              pos = 0;
              length = buffer.length;
              startByte += buffer.length;
            } else {
              // Usually end of file.
              pos += size;
              length = buffer.length - pos;
            }
            size = bufferedStream.read(buffer, pos, length);
          }
        } finally {
          IOUtils.closeQuietly(bufferedStream);
        }
      }

      // Final commit of buffer.
      if (pos > 0) {
            uploadLogPart(connection, execId, name, attempt, startByte, startByte
            + pos, encType, buffer, pos);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error writing log part.", e);
    } catch (final IOException e) {
      throw new ExecutorManagerException("Error chunking", e);
    }
  }

  int removeExecutionLogsByTime(final long millis)
      throws ExecutorManagerException {
    final String DELETE_BY_TIME =
        "DELETE FROM execution_logs WHERE upload_time < ?";

    final QueryRunner runner = this.createQueryRunner();
    int updateNum = 0;
    try {
      updateNum = runner.update(DELETE_BY_TIME, millis);
    } catch (final SQLException e) {
      e.printStackTrace();
      throw new ExecutorManagerException(
          "Error deleting old execution_logs before " + millis, e);
    }

    return updateNum;
  }

  // TODO kunkun-tang: Will be removed in the future refactor.
  private EncodingType getDefaultEncodingType() {
    return this.defaultEncodingType;
  }

  private void uploadLogPart(final Connection connection, final int execId, final String name,
                             final int attempt, final int startByte, final int endByte,
                             final EncodingType encType,
                             final byte[] buffer, final int length) throws SQLException, IOException {
    final String INSERT_EXECUTION_LOGS =
        "INSERT INTO execution_logs "
            + "(exec_id, name, attempt, enc_type, start_byte, end_byte, "
            + "log, upload_time) VALUES (?,?,?,?,?,?,?,?)";

    final QueryRunner runner = new QueryRunner();
    byte[] buf = buffer;
    if (encType == EncodingType.GZIP) {
      buf = GZIPUtils.gzipBytes(buf, 0, length);
    } else if (length < buf.length) {
      buf = Arrays.copyOf(buffer, length);
    }

    runner.update(connection, INSERT_EXECUTION_LOGS, execId, name, attempt,
        encType.getNumVal(), startByte, startByte + length, buf, DateTime.now()
            .getMillis());
  }

  // TODO kunkun-tang: should be removed in future refactor.
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

  private static class FetchLogsHandler implements ResultSetHandler<LogData> {

    private static final String FETCH_LOGS =
        "SELECT exec_id, name, attempt, enc_type, start_byte, end_byte, log "
            + "FROM execution_logs "
            + "WHERE exec_id=? AND name=? AND attempt=? AND end_byte > ? "
            + "AND start_byte <= ? ORDER BY start_byte";

    private final int startByte;
    private final int endByte;

    FetchLogsHandler(final int startByte, final int endByte) {
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
}
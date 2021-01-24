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
import azkaban.db.DatabaseTransOperator;
import azkaban.db.EncodingType;
import azkaban.db.SQLTransaction;
import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.GZIPUtils;
import azkaban.utils.Pair;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;


@Singleton
public class ExecutionLogsDao {

  private static final Logger logger = Logger.getLogger(ExecutionLogsDao.class);
  private final DatabaseOperator dbOperator;
  private final EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  ExecutionLogsDao(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  // TODO kunkun-tang: the interface's parameter is called endByte, but actually is length.
  LogData fetchLogs(final int execId, final String name, final int attempt,
      final int startByte,
      final int length) throws ExecutorManagerException {
    final FetchLogsHandler handler = new FetchLogsHandler(startByte, length + startByte);
    try {
      return this.dbOperator.query(FetchLogsHandler.FETCH_LOGS, handler,
          execId, name, attempt, startByte, startByte + length);
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching logs " + execId
          + " : " + name, e);
    }
  }

  public void uploadLogFile(final int execId, final String name, final int attempt,
      final File... files) throws ExecutorManagerException {
    final SQLTransaction<Integer> transaction = transOperator -> {
      uploadLogFile(transOperator, execId, name, attempt, files, this.defaultEncodingType);
      transOperator.getConnection().commit();
      return 1;
    };
    try {
      this.dbOperator.transaction(transaction);
    } catch (final SQLException e) {
      logger.error("uploadLogFile failed.", e);
      throw new ExecutorManagerException("uploadLogFile failed.", e);
    }
  }

  private void uploadLogFile(final DatabaseTransOperator transOperator, final int execId,
      final String name,
      final int attempt, final File[] files, final EncodingType encType)
      throws SQLException {
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
              uploadLogPart(transOperator, execId, name, attempt, startByte,
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
        uploadLogPart(transOperator, execId, name, attempt, startByte, startByte
            + pos, encType, buffer, pos);
      }
    } catch (final SQLException e) {
      logger.error("Error writing log part.", e);
      throw new SQLException("Error writing log part", e);
    } catch (final IOException e) {
      logger.error("Error chunking.", e);
      throw new SQLException("Error chunking", e);
    }
  }

  int removeExecutionLogsByTime(final long millis, final int recordCleanupLimit)
      throws ExecutorManagerException {
    int totalRecordsRemoved = 0;
    int removedRecords;
    do {
      removedRecords = removeExecutionLogsBatch(millis, recordCleanupLimit);
      logger.debug("Removed batch of execution logs. Count of records removed in this batch: "
          + removedRecords);
      totalRecordsRemoved = totalRecordsRemoved + removedRecords;
      // Adding sleep of 1 second
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        logger.error("Execution logs cleanup thread's sleep was interrupted.", e);
      }
    } while (removedRecords == recordCleanupLimit);
    return totalRecordsRemoved;
  }

  int removeExecutionLogsBatch(final long millis, final int recordCleanupLimit)
      throws ExecutorManagerException {
    final String DELETE_BY_TIME =
        "DELETE FROM execution_logs WHERE upload_time IN (SELECT upload_time FROM execution_logs WHERE upload_time < ? LIMIT ?)";
    try {
      return this.dbOperator.update(DELETE_BY_TIME, millis, recordCleanupLimit);
    } catch (final SQLException e) {
      logger.error("delete execution logs failed", e);
      throw new ExecutorManagerException(
          "Error deleting old execution_logs before " + millis, e);
    }
  }

  private void uploadLogPart(final DatabaseTransOperator transOperator, final int execId,
      final String name,
      final int attempt, final int startByte, final int endByte,
      final EncodingType encType,
      final byte[] buffer, final int length)
      throws SQLException, IOException {
    final String INSERT_EXECUTION_LOGS = "INSERT INTO execution_logs "
        + "(exec_id, name, attempt, enc_type, start_byte, end_byte, "
        + "log, upload_time) VALUES (?,?,?,?,?,?,?,?)";

    byte[] buf = buffer;
    if (encType == EncodingType.GZIP) {
      buf = GZIPUtils.gzipBytes(buf, 0, length);
    } else if (length < buf.length) {
      buf = Arrays.copyOf(buffer, length);
    }

    transOperator.update(INSERT_EXECUTION_LOGS, execId, name, attempt,
        encType.getNumVal(), startByte, startByte + length, buf, DateTime.now()
            .getMillis());
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

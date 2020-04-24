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

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.db.DatabaseOperator;
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.FileIOUtils.LogData;
import java.io.File;
import java.sql.SQLException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutionLogsDaoTest {

  private static final String LOG_TEST_DIR_NAME = "logtest";
  private static DatabaseOperator dbOperator;
  private ExecutionLogsDao executionLogsDao;

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = Utils.initTestDB();
  }

  @AfterClass
  public static void destroyDB() throws Exception {
    try {
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() {
    this.executionLogsDao = new ExecutionLogsDao(dbOperator);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("delete from execution_logs");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSmallUploadLog() throws ExecutorManagerException {
    final File logDir = ExecutionsTestUtil.getFlowDir(LOG_TEST_DIR_NAME);
    final File[] smalllog =
        {new File(logDir, "log1.log"), new File(logDir, "log2.log"),
            new File(logDir, "log3.log")};

    this.executionLogsDao.uploadLogFile(1, "smallFiles", 0, smalllog);

    final LogData data = this.executionLogsDao.fetchLogs(1, "smallFiles", 0, 0, 50000);
    assertThat(data).isNotNull();
    assertThat(data.getLength()).isEqualTo(53);
    System.out.println(data.toString());

    final LogData data2 = this.executionLogsDao.fetchLogs(1, "smallFiles", 0, 10, 20);
    System.out.println(data2.toString());

    assertThat(data2).isNotNull();
    assertThat(data2.getLength()).isEqualTo(20);
  }

  @Test
  public void testLargeUploadLog() throws ExecutorManagerException {
    final File logDir = ExecutionsTestUtil.getFlowDir(LOG_TEST_DIR_NAME);

    // Multiple of 255 for Henry the Eigth
    final File[] largelog =
        {new File(logDir, "largeLog1.log"), new File(logDir, "largeLog2.log"),
            new File(logDir, "largeLog3.log")};

    this.executionLogsDao.uploadLogFile(1, "largeFiles", 0, largelog);

    final LogData logsResult = this.executionLogsDao.fetchLogs(1, "largeFiles", 0, 0, 64000);
    assertThat(logsResult).isNotNull();
    assertThat(logsResult.getLength()).isEqualTo(64000);

    final LogData logsResult2 = this.executionLogsDao.fetchLogs(1, "largeFiles", 0, 1000, 64000);
    assertThat(logsResult2).isNotNull();
    assertThat(logsResult2.getLength()).isEqualTo(64000);

    final LogData logsResult3 = this.executionLogsDao.fetchLogs(1, "largeFiles", 0, 150000, 250000);
    assertThat(logsResult3).isNotNull();
    assertThat(logsResult3.getLength()).isEqualTo(185493);
  }

  @Test
  public void testLogCleanup() throws ExecutorManagerException {
    final File logDir = ExecutionsTestUtil.getFlowDir(LOG_TEST_DIR_NAME);
    // Multiple of 255 for Henry the Eigth
    final File[] largeLog1 =
        {new File(logDir, "largeLog1.log")};

    this.executionLogsDao.uploadLogFile(1, "largeFiles", 0, largeLog1);

    final long currentTimeMillis = System.currentTimeMillis() + 1000;
    int totalRemovedRecords = executionLogsDao.removeExecutionLogsByTime(currentTimeMillis, 2);
    assertThat(totalRemovedRecords).isEqualTo(3);

    // Multiple of 255 for Henry the Eigth
    final File[] largeLog2 =
        {new File(logDir, "largeLog2.log")};

    this.executionLogsDao.uploadLogFile(2, "largeFiles", 0, largeLog2);

    final long currentTimeMillisSecond = System.currentTimeMillis() + 1000;
    totalRemovedRecords = executionLogsDao.removeExecutionLogsByTime(currentTimeMillisSecond, 3);
    assertThat(totalRemovedRecords).isEqualTo(1);

    // Multiple of 255 for Henry the Eigth
    final File[] largeLogMultiple =
        {new File(logDir, "largeLog2.log")};

    this.executionLogsDao.uploadLogFile(3, "largeFiles", 0, largeLogMultiple);

    // Multiple of 255 for Henry the Eigth
    final File[] largeLog4 =
        {new File(logDir, "largeLog1.log")};

    this.executionLogsDao.uploadLogFile(4, "largeFiles", 0, largeLog4);

    final long currentTimeMillis2 = System.currentTimeMillis() + 1000;
    totalRemovedRecords = executionLogsDao.removeExecutionLogsByTime(currentTimeMillis2, 2);
    assertThat(totalRemovedRecords).isEqualTo(4);
  }
}

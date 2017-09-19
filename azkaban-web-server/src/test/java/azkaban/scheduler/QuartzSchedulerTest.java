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

package azkaban.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.db.DatabaseOperator;
import azkaban.test.Utils;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.SchedulerException;

/**
 * Use H2-in-mem database to directly test Quartz.
 */
public class QuartzSchedulerTest {

  private static DatabaseOperator dbOperator;
  private static QuartzScheduler scheduler;

  @BeforeClass
  public static void setUpQuartz() throws Exception {
    dbOperator = Utils.initQuartzDB();
    final String quartzPropsPath=
        new File("../azkaban-web-server/src/test/resources/quartz.test.properties")
        .getCanonicalPath();
    final Props quartzProps = new Props(null, quartzPropsPath);
    scheduler = new QuartzScheduler(quartzProps);
    scheduler.start();
  }

  @AfterClass
  public static void destroyQuartz() {
    try {
      scheduler.shutdown();
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void init() {
    TestQuartzJob.COUNT_EXECUTION = 0;
  }

  @After
  public void cleanup() {
    scheduler.cleanup();
  }

  @Test
  public void testCreateScheduleAndRun() throws Exception{
    scheduler.registerJob("* * * * * ?", createJobDescription());
    assertThat(scheduler.ifJobExist("TestService")).isEqualTo(true);
    TestUtils.await().untilAsserted(() -> assertThat(TestQuartzJob.COUNT_EXECUTION)
        .isNotNull().isGreaterThan(1));
  }

  @Test
  public void testNotAllowDuplicateJobRegister() throws Exception{
    scheduler.registerJob("* * * * * ?", createJobDescription());
    assertThatThrownBy(
        () -> scheduler.registerJob("0 5 * * * ?", createJobDescription()))
        .isInstanceOf(SchedulerException.class)
        .hasMessageContaining("can not register existing job");
  }

  @Test
  public void testInvalidCron() throws Exception{
    assertThatThrownBy(
        () -> scheduler.registerJob("0 5 * * * *", createJobDescription()))
        .isInstanceOf(SchedulerException.class)
        .hasMessageContaining("The cron expression string");
  }

  @Test
  public void testUnregisterSchedule() throws Exception{
    scheduler.registerJob("* * * * * ?", createJobDescription());
    assertThat(scheduler.ifJobExist("TestService")).isEqualTo(true);
    scheduler.unregisterJob("TestService");
    assertThat(scheduler.ifJobExist("TestService")).isEqualTo(false);
  }

  @Test
  public void testPauseAndResume() throws Exception{
    scheduler.registerJob("* * * * * ?", createJobDescription());
    scheduler.pause();
    final int count = TestQuartzJob.COUNT_EXECUTION;
    Thread.sleep(1500);
    assertThat(TestQuartzJob.COUNT_EXECUTION).isEqualTo(count);
    scheduler.resume();
    Thread.sleep(1200);
    assertThat(TestQuartzJob.COUNT_EXECUTION).isGreaterThan(count);
  }

  private QuartzJobDescription createJobDescription() {
    final TestService testService = new TestService("first field", "second field");
    final Map<String, TestService> contextMap = new HashMap<>();
    contextMap.put(TestQuartzJob.DELEGATE_CLASS_NAME, testService);
    return new QuartzJobDescription(TestQuartzJob.class,
        "TestService",
        contextMap);
  }
}

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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.AzkabanCommonModule;
import azkaban.Constants.ConfigurationKeys;
import azkaban.db.AzDBTestUtility;
import azkaban.db.DatabaseOperator;
import azkaban.test.TestUtils;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServerModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
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
    dbOperator = AzDBTestUtility.initQuartzDB();
    final String quartzPropsPath =
        new File("../azkaban-web-server/src/test/resources/quartz.test.properties")
            .getCanonicalPath();
    final Props props = new Props(null, quartzPropsPath);
    props.put(ConfigurationKeys.ENABLE_QUARTZ, "true");

    props.put("database.type", "h2");
    props.put("h2.path", "./h2");
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerModule(props)
    );

    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(injector);

    scheduler = new QuartzScheduler(props);
    scheduler.start();
  }

  @AfterClass
  public static void destroyQuartz() {
    try {
      scheduler.shutdown();
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Before
  public void init() {
    SampleQuartzJob.COUNT_EXECUTION = 0;
  }

  @After
  public void cleanup() throws SchedulerException {
    scheduler.cleanup();
  }

  @Test
  public void testCreateScheduleAndRun() throws Exception {
    scheduler.scheduleIfAbsent("* * * * * ?", createJobDescription());
    assertThat(scheduler.ifJobExist("SampleJob", "SampleService")).isEqualTo(true);
    TestUtils.await().untilAsserted(() -> assertThat(SampleQuartzJob.COUNT_EXECUTION)
        .isNotNull().isGreaterThan(1));
  }

  @Test
  public void testSchedulingDuplicateJob() throws Exception {
    scheduler.scheduleIfAbsent("* * * * * ?", createJobDescription());
    assertThat(scheduler.scheduleIfAbsent("0 5 * * * ?", createJobDescription())).isFalse();
  }

  @Test
  public void testInvalidCron() {
    assertThatThrownBy(
        () -> scheduler.scheduleIfAbsent("0 5 * * * *", createJobDescription()))
        .isInstanceOf(SchedulerException.class)
        .hasMessageContaining("The cron expression string");
  }

  @Test
  public void testUnschedule() throws Exception {
    scheduler.scheduleIfAbsent("* * * * * ?", createJobDescription());
    assertThat(scheduler.ifJobExist("SampleJob", "SampleService")).isEqualTo(true);
    assertThat(scheduler.unschedule("SampleJob", "SampleService")).isTrue();
    assertThat(scheduler.ifJobExist("SampleJob", "SampleService")).isEqualTo(false);
    assertThat(scheduler.unschedule("SampleJob", "SampleService")).isFalse();
  }

  @Test
  public void testPauseSchedule() throws Exception {
    assertThat(scheduler.pauseJobIfPresent("SampleJob", "SampleService")).isFalse();
    scheduler.scheduleIfAbsent("* * * * * ?", createJobDescription());
    assertThat(scheduler.pauseJobIfPresent("SampleJob", "SampleService")).isTrue();
    assertThat(scheduler.isJobPaused("SampleJob", "SampleService")).isEqualTo(true);
    assertThat(scheduler.resumeJobIfPresent("SampleJob", "SampleService")).isTrue();
    assertThat(scheduler.isJobPaused("SampleJob", "SampleService")).isEqualTo(false);

    // test pausing a paused job
    scheduler.pauseJobIfPresent("SampleJob", "SampleService");
    scheduler.pauseJobIfPresent("SampleJob", "SampleService");
    assertThat(scheduler.isJobPaused("SampleJob", "SampleService")).isEqualTo(true);
    // test resuming a non-paused job
    scheduler.resumeJobIfPresent("SampleJob", "SampleService");
    scheduler.resumeJobIfPresent("SampleJob", "SampleService");
    assertThat(scheduler.isJobPaused("SampleJob", "SampleService")).isEqualTo(false);
  }

  private QuartzJobDescription createJobDescription() {
    return new QuartzJobDescription<>(SampleQuartzJob.class, "SampleJob", "SampleService");
  }
}

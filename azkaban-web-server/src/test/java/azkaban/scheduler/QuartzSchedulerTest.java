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

import azkaban.db.DatabaseOperator;
import azkaban.test.Utils;
import azkaban.utils.Props;
import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
public class QuartzSchedulerTest {

  private static DatabaseOperator dbOperator;
  private static QuartzScheduler scheduler;

  @BeforeClass
  public static void setUpQUartz() throws Exception {
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

  @After
  public void cleanup() {
    scheduler.cleanup();
  }

  @Test
  public void test1() throws Exception{
    scheduler.register("* * * * * ?", createJobDescription());
    assertThat(scheduler.ifJobExist("TestService")).isEqualTo(true);
    Thread.sleep(5000);
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

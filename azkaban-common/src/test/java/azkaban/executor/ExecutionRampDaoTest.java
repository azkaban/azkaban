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
import azkaban.utils.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class ExecutionRampDaoTest {

  private static DatabaseOperator dbOperator;
  private ExecutionRampDao executionRampDao;

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
    this.executionRampDao = new ExecutionRampDao(dbOperator);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("DELETE FROM ramp");
      dbOperator.update("DELETE FROM ramp_items");
      dbOperator.update("DELETE FROM ramp_dependency");
      dbOperator.update("DELETE FROM ramp_exceptional_flow_items");
      dbOperator.update("DELETE FROM ramp_exceptional_job_items");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsertFetchDeleteExecutableRampMap() throws Exception {

    Map<String, Object> data = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .put("isActive", 1)
        .put("rampPolicy", "SimpleAutoRampPolicy")
        .put("startTime", 0L)
        .put("maxFailureToPause", 5)
        .put("maxFailureToRampDown", 10)
        .build();
    this.executionRampDao.insertAction("ramp", data);

    final ExecutableRampMap record =
        this.executionRampDao.fetchExecutableRampMap();

    assertThat(record.get("dali").isActive()).isTrue();
    assertThat(record.get("dali").getPolicy()).isEqualTo("SimpleAutoRampPolicy");
    assertThat(record.get("dali").getMaxFailureToRampDown()).isEqualTo(10);
    assertThat(record.get("dali").getMaxFailureToPause()).isEqualTo(5);
    assertThat(record.get("dali").isPercentageScaleForMaxFailure()).isFalse();
    assertThat(record.get("dali").getStage()).isEqualTo(0);
    assertThat(record.get("dali").getStartTime()).isEqualTo(0);
    assertThat(record.get("dali").getEndTime()).isEqualTo(0);
    assertThat(record.get("dali").getLastUpdatedTime()).isEqualTo(0);
    assertThat(record.get("dali").getCount(ExecutableRamp.CountType.FAILURE)).isEqualTo(0);
    assertThat(record.get("dali").getCount(ExecutableRamp.CountType.SUCCESS)).isEqualTo(0);
    assertThat(record.get("dali").getCount(ExecutableRamp.CountType.TRAIL)).isEqualTo(0);
    assertThat(record.get("dali").isPaused()).isFalse();

    Map<String, Object> conditionData = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .build();

    this.executionRampDao.deleteAction("ramp", conditionData);

    final ExecutableRampMap result = this.executionRampDao.fetchExecutableRampMap();

    assertThat(result.size()).isZero();
  }

  @Test
  public void testInsertFetchDeleteExecutableRampItemsMap() throws Exception {

    Map<String, Object> data = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .put("dependency", "jar:dali-pig-data")
        .put("rampValue", "/export/lib/dali-pig-data-9.12.1.jar")
        .build();
    this.executionRampDao.insertAction("ramp_items", data);

    final ExecutableRampItemsMap record = this.executionRampDao.fetchExecutableRampItemsMap();

    assertThat(record.getRampItems("dali").get("jar:dali-pig-data"))
        .isEqualToIgnoringWhitespace("/export/lib/dali-pig-data-9.12.1.jar");

    Map<String, Object> conditionData = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .build();

    this.executionRampDao.deleteAction("ramp_items", conditionData);

    final ExecutableRampItemsMap result = this.executionRampDao.fetchExecutableRampItemsMap();

    assertThat(result.size()).isZero();
  }

  @Test
  public void testInsertFetchDeleteExecutableRampDependencyMap() throws Exception {

    Map<String, Object> data = ImmutableMap.<String, Object>builder()
        .put("dependency", "jar:dali-data-spark")
        .build();
    this.executionRampDao.insertAction("ramp_dependency", data);

    final ExecutableRampDependencyMap record =
        this.executionRampDao.fetchExecutableRampDependencyMap();

    assertThat(record.getDefaultValue("jar:dali-data-spark")).isNullOrEmpty();
    assertThat(record.get("jar:dali-data-spark").getAssociatedJobTypes()).isNullOrEmpty();


    Map<String, Object> conditionData = ImmutableMap.<String, Object>builder()
        .put("dependency", "jar:dali-data-spark")
        .build();

    this.executionRampDao.deleteAction("ramp_dependency", conditionData);

    final ExecutableRampDependencyMap result = this.executionRampDao.fetchExecutableRampDependencyMap();

    assertThat(result.size()).isZero();
  }

  @Test
  public void testInsertFetchDeleteExecutableRampExceptionalFlowItemsMap() throws Exception {

    Map<String, Object> data = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .put("flowId", "contact-join.cjn-all")
        .put("treatment", "B")
        .build();
    this.executionRampDao.insertAction("ramp_exceptional_flow_items", data);

    Map<String, Object> data2 = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .put("flowId", "contact-join.cjn-all2")
        .put("treatment", "w")
        .put("timestamp", 1566518763000L)
        .build();
    this.executionRampDao.insertAction("ramp_exceptional_flow_items", data2);

    final ExecutableRampExceptionalFlowItemsMap record =
        this.executionRampDao.fetchExecutableRampExceptionalFlowItemsMap();

    assertThat(record.get("dali").getItems().size()).isEqualTo(2);
    assertThat(record.get("dali").getItems().get("contact-join.cjn-all").getStatus())
        .isEqualByComparingTo(ExecutableRampStatus.BLACKLISTED);
    assertThat(record.get("dali").getItems().get("contact-join.cjn-all").getTimeStamp())
        .isNotZero();
    assertThat(record.get("dali").getItems().get("contact-join.cjn-all2").getStatus())
        .isEqualByComparingTo(ExecutableRampStatus.WHITELISTED);
    assertThat(record.get("dali").getItems().get("contact-join.cjn-all2").getTimeStamp())
        .isNotZero();

    Map<String, Object> conditionData = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .build();

    this.executionRampDao.deleteAction("ramp_exceptional_flow_items", conditionData);

    final ExecutableRampExceptionalFlowItemsMap result = this.executionRampDao.fetchExecutableRampExceptionalFlowItemsMap();

    assertThat(result.size()).isZero();
  }

  @Test
  public void testInsertFetchDeleteExecutableRampExceptionalJobItemsMap() throws Exception {

    Map<String, Object> data = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .put("flowId", "contact-join.cjn-all")
        .put("jobId", "job1")
        .put("treatment", "B")
        .build();
    this.executionRampDao.insertAction("ramp_exceptional_job_Items", data);

    Map<String, Object> data2 = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .put("flowId", "contact-join.cjn-all")
        .put("jobId", "job2")
        .put("treatment", "w")
        .put("timestamp", 1566518763000L)
        .build();
    this.executionRampDao.insertAction("ramp_exceptional_job_Items", data2);

    final ExecutableRampExceptionalJobItemsMap record =
        this.executionRampDao.fetchExecutableRampExceptionalJobItemsMap();

    Pair<String, String> key = new Pair<>("dali", "contact-join.cjn-all");
    assertThat(record.get(key).getItems().size()).isEqualTo(2);
    assertThat(record.get(key).getItems().get("job1").getStatus())
        .isEqualByComparingTo(ExecutableRampStatus.BLACKLISTED);
    assertThat(record.get(key).getItems().get("job1").getTimeStamp())
        .isNotZero();
    assertThat(record.get(key).getItems().get("job2").getStatus())
        .isEqualByComparingTo(ExecutableRampStatus.WHITELISTED);
    assertThat(record.get(key).getItems().get("job2").getTimeStamp())
        .isNotZero();

    Map<String, Object> conditionData = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .build();

    this.executionRampDao.deleteAction("ramp_exceptional_job_Items", conditionData);

    final ExecutableRampExceptionalJobItemsMap result = this.executionRampDao.fetchExecutableRampExceptionalJobItemsMap();

    assertThat(result.size()).isZero();
  }

  @Test
  public void testDoRampActions() throws ExecutorManagerException {
    List<Map<String, Object>> actions = ImmutableList.<Map<String, Object>>builder()
        .add(ImmutableMap.<String, Object>builder()
            .put("action", "insert")
            .put("table", "ramp")
            .put("values", ImmutableMap.<String, Object>builder()
                .put("rampId", "dali")
                .put("rampPolicy", "SimpleAutoRampPolicy")
                .build())
            .build())
        .add(ImmutableMap.<String, Object>builder()
            .put("action", "insert")
            .put("table", "ramp_items")
            .put("values", ImmutableMap.<String, Object>builder()
                .put("rampId", "dali")
                .put("dependency", "jar:dali-data-pig")
                .put("rampValue", "/export/apps/dali/pig/dali-data-pig-9.2.10.jar")
                .build())
            .build())
        .build();

    Map<String, String> results = this.executionRampDao.doRampActions(actions);

    results.forEach((k, v) -> assertThat(v.startsWith("[SUCCESS]")).isTrue());

    final ExecutableRampMap ramps =
        this.executionRampDao.fetchExecutableRampMap();

    assertThat(ramps.get("dali").isActive()).isFalse();
    assertThat(ramps.get("dali").getPolicy()).isEqualTo("SimpleAutoRampPolicy");
    assertThat(ramps.get("dali").getMaxFailureToRampDown()).isZero();
    assertThat(ramps.get("dali").getMaxFailureToPause()).isZero();
    assertThat(ramps.get("dali").isPercentageScaleForMaxFailure()).isFalse();
    assertThat(ramps.get("dali").getStage()).isZero();
    assertThat(ramps.get("dali").getStartTime()).isNotZero();
    assertThat(ramps.get("dali").getEndTime()).isZero();
    assertThat(ramps.get("dali").getLastUpdatedTime()).isZero();
    assertThat(ramps.get("dali").getCount(ExecutableRamp.CountType.FAILURE)).isZero();
    assertThat(ramps.get("dali").getCount(ExecutableRamp.CountType.SUCCESS)).isZero();
    assertThat(ramps.get("dali").getCount(ExecutableRamp.CountType.TRAIL)).isZero();
    assertThat(ramps.get("dali").isPaused()).isFalse();
    assertThat(ramps.size()).isEqualTo(1);

    final ExecutableRampItemsMap rampItems = this.executionRampDao.fetchExecutableRampItemsMap();

    assertThat(rampItems.getRampItems("dali").get("jar:dali-data-pig"))
        .isEqualToIgnoringWhitespace("/export/apps/dali/pig/dali-data-pig-9.2.10.jar");
    assertThat(rampItems.size()).isEqualTo(1);
  }

  @Test
  public void testUpdateExecutableRamp() throws ExecutorManagerException {

    Map<String, Object> data = ImmutableMap.<String, Object>builder()
        .put("rampId", "dali")
        .put("isActive", 1)
        .put("rampPolicy", "SimpleAutoRampPolicy")
        .put("startTime", 0L)
        .put("maxFailureToPause", 5)
        .put("maxFailureToRampDown", 10)
        .build();
    this.executionRampDao.insertAction("ramp", data);

    final ExecutableRampMap record =
        this.executionRampDao.fetchExecutableRampMap();

    assertThat(record.get("dali").isActive()).isTrue();
    assertThat(record.get("dali").getPolicy()).isEqualTo("SimpleAutoRampPolicy");
    assertThat(record.get("dali").getMaxFailureToRampDown()).isEqualTo(10);
    assertThat(record.get("dali").getMaxFailureToPause()).isEqualTo(5);
    assertThat(record.get("dali").isPercentageScaleForMaxFailure()).isFalse();
    assertThat(record.get("dali").getStage()).isEqualTo(0);
    assertThat(record.get("dali").getStartTime()).isEqualTo(0);
    assertThat(record.get("dali").getEndTime()).isEqualTo(0);
    assertThat(record.get("dali").getLastUpdatedTime()).isEqualTo(0);
    assertThat(record.get("dali").getCount(ExecutableRamp.CountType.FAILURE)).isEqualTo(0);
    assertThat(record.get("dali").getCount(ExecutableRamp.CountType.SUCCESS)).isEqualTo(0);
    assertThat(record.get("dali").getCount(ExecutableRamp.CountType.TRAIL)).isEqualTo(0);
    assertThat(record.get("dali").isPaused()).isFalse();

    record.get("dali").cacheResult(ExecutableRamp.Action.FAILED);
    record.get("dali").cacheResult(ExecutableRamp.Action.SUCCEEDED);
    record.get("dali").cacheResult(ExecutableRamp.Action.SUCCEEDED);
    record.get("dali").cacheResult(ExecutableRamp.Action.SUCCEEDED);
    record.get("dali").cacheResult(ExecutableRamp.Action.IGNORED);

    this.executionRampDao.updateExecutableRamp(record.get("dali"));

    final ExecutableRampMap updatedRecord =
        this.executionRampDao.fetchExecutableRampMap();

    assertThat(updatedRecord.get("dali").getCount(ExecutableRamp.CountType.FAILURE)).isEqualTo(1);
    assertThat(updatedRecord.get("dali").getCount(ExecutableRamp.CountType.SUCCESS)).isEqualTo(3);
    assertThat(updatedRecord.get("dali").getCount(ExecutableRamp.CountType.TRAIL)).isEqualTo(5);
    assertThat(updatedRecord.get("dali").getCount(ExecutableRamp.CountType.IGNORED)).isEqualTo(1);
    assertThat(updatedRecord.get("dali").getLastUpdatedTime()).isGreaterThan(0);
  }

  @Test
  public void testUpdateExecutedRampFlows() throws ExecutorManagerException {
    long timeStamp = System.currentTimeMillis();

    ExecutableRampExceptionalItems items = ExecutableRampExceptionalItems
        .createInstance()
        .add("contact-join.cjn-all", ExecutableRampStatus.SELECTED, timeStamp, true)
        .add("project.flow", ExecutableRampStatus.UNSELECTED, timeStamp, true);
    this.executionRampDao.updateExecutedRampFlows("dali", items);

    final ExecutableRampExceptionalFlowItemsMap record =
        this.executionRampDao.fetchExecutableRampExceptionalFlowItemsMap();

    assertThat(record.get("dali").getItems().size()).isEqualTo(2);
    assertThat(record.get("dali").getItems().get("contact-join.cjn-all").getStatus())
        .isEqualByComparingTo(ExecutableRampStatus.SELECTED);
    assertThat(record.get("dali").getItems().get("contact-join.cjn-all").getTimeStamp())
        .isEqualTo(timeStamp);
    assertThat(record.get("dali").getItems().get("project.flow").getStatus())
        .isEqualByComparingTo(ExecutableRampStatus.UNSELECTED);
    assertThat(record.get("dali").getItems().get("project.flow").getTimeStamp())
        .isEqualTo(timeStamp);
  }
}

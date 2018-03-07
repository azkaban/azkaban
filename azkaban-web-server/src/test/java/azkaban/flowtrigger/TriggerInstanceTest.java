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

package azkaban.flowtrigger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.junit.Test;

public class TriggerInstanceTest {

  public static Date getDate(final int hour, final int minute, final int second) {
    final Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, 2000);
    cal.set(Calendar.MONTH, 1);
    cal.set(Calendar.DAY_OF_MONTH, 1);
    cal.set(Calendar.HOUR_OF_DAY, hour);
    cal.set(Calendar.MINUTE, minute);
    cal.set(Calendar.SECOND, second);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  private DependencyInstance createTestDependencyInstance(final Status status,
      final CancellationCause killingCause) {
    final DependencyInstance depInst = new DependencyInstance(null, 0, 0, null, null,
        null);
    depInst.setStatus(status);
    depInst.setCancellationCause(killingCause);
    return depInst;
  }

  private DependencyInstance createTestDependencyInstance(final Status status,
      final CancellationCause cancelCause, final Date startTime, final Date endTime) {
    final DependencyInstance depInst = new DependencyInstance(null, startTime.getTime(), endTime
        == null ? 0 : endTime.getTime(), null, status, cancelCause);
    depInst.setStatus(status);
    depInst.setCancellationCause(cancelCause);
    return depInst;
  }

  @Test
  public void testTriggerInstanceStartTime() throws Exception {
    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    Date expectedStartTime = getDate(2, 2, 2);
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(2, 2,
            2), getDate(2, 2, 3)));

    TriggerInstance ti = null;
    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);
    assertThat(ti.getStartTime()).isEqualTo(expectedStartTime.getTime());
    dependencyInstanceList.clear();

    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);
    assertThat(ti.getStartTime()).isEqualTo(0);
    dependencyInstanceList.clear();

    expectedStartTime = getDate(2, 2, 2);
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(2, 2,
            4), getDate(2, 2, 3)));

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(2, 2,
            3), getDate(2, 2, 3)));

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(2, 2,
            2), getDate(2, 2, 3)));

    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);
    assertThat(ti.getStartTime()).isEqualTo(expectedStartTime.getTime());
  }

  @Test
  public void testTriggerInstanceEndTime() throws Exception {
    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    Date expectedEndTime = getDate(3, 2, 3);
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(2, 2,
            2), getDate(3, 2, 3)));

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(2, 2,
            2), getDate(2, 2, 3)));

    TriggerInstance ti = null;
    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);
    assertThat(ti.getEndTime()).isEqualTo(expectedEndTime.getTime());
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.NONE, getDate(2, 2,
            2), null));

    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);
    assertThat(ti.getEndTime()).isEqualTo(0);
    dependencyInstanceList.clear();

    expectedEndTime = getDate(3, 2, 3);
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(2, 2,
            3), null));

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(3, 2,
            2), null));

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE, getDate(2, 2,
            2), expectedEndTime));
    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);
    assertThat(ti.getEndTime()).isEqualTo(expectedEndTime.getTime());
    dependencyInstanceList.clear();

    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);
    assertThat(ti.getEndTime()).isEqualTo(0);
    dependencyInstanceList.clear();
  }

  @Test
  public void testTriggerInstanceRunningStatus() throws Exception {
    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    TriggerInstance ti = null;
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.RUNNING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.MANUAL));

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.RUNNING);
    dependencyInstanceList.clear();
  }

  @Test
  public void testTriggerInstanceSucceededStatus() throws Exception {
    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    TriggerInstance ti = null;

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.SUCCEEDED);
    dependencyInstanceList.clear();

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.SUCCEEDED);
    dependencyInstanceList.clear();
  }

  @Test
  public void testTriggerInstanceCancellingStatus() throws Exception {
    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    TriggerInstance ti = null;
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLING, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLING, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.RUNNING, CancellationCause.NONE));

    ti = new TriggerInstance("1", null,
        "1", 1, "test", dependencyInstanceList,
        -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLING);
    dependencyInstanceList.clear();
  }

  @Test
  public void testTriggerInstanceCancelledStatus() throws Exception {
    final List<DependencyInstance> dependencyInstanceList = new ArrayList<>();
    TriggerInstance ti = null;
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.SUCCEEDED, CancellationCause.NONE));

    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);

    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLED);
    dependencyInstanceList.clear();

    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));
    dependencyInstanceList
        .add(createTestDependencyInstance(Status.CANCELLED, CancellationCause.MANUAL));

    ti = new TriggerInstance("1", null, "1", 1,
        "test", dependencyInstanceList, -1, null);
    assertThat(ti.getStatus()).isEqualTo(Status.CANCELLED);
    dependencyInstanceList.clear();
  }

}

/*
 * Copyright 2018 LinkedIn Corp.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

public class ScheduleManagerTest {

  @Mock
  private static ScheduleLoader loader;

  private static ScheduleManager manager;

  @BeforeClass
  public static void setUpClass() throws Exception {
    loader = mock(ScheduleLoader.class);

    List<Schedule> schedules = new ArrayList<>();

    Schedule s1 = new Schedule(1, 1, "PROJECT_A_V1", "FLOW_A", "NOT_EXPIRED", -1, -1, null, null,
        -1,
        DateTime.now().getMillis(), -1, "AzkaBAN", null, null, null);
    Schedule s2 = new Schedule(2, 1, "PROJECT_B_V1", "FLOW_B", "NOT_EXPIRED", -1, -1, null,
        null, -1,
        DateTime.now().getMillis(), -1, "azKAbaN", null, null, null);
    Schedule s3 = new Schedule(3, 1, "PROJECT_AB_V1", "FLOW_AB", "NOT_EXPIRED", -1, -1, null,
        null, -1,
        DateTime.now().getMillis(), -1, "some_user", null, null, null);
    Schedule s4 = new Schedule(4, 1, "PROJECT_ZZ_V1", "FLOW_ZZ", "NOT_EXPIRED", -1, -1, null,
        null,
        -1,
        DateTime.now().getMillis(), -1, "some_user2", null, null, null);

    schedules.add(s1);
    schedules.add(s2);
    schedules.add(s3);
    schedules.add(s4);

    when(loader.loadUpdatedSchedules()).thenReturn(schedules);

    manager = new ScheduleManager(loader);
  }

  @Test
  public void testProjectNameFilter() throws Exception {
    List<Schedule> schedules = manager.getSchedules("A", "",
        "", -1, -1);

    Assert.assertEquals(2, schedules.size());

    schedules = manager.getSchedules("a", "",
        "", -1, -1);

    Assert.assertEquals(2, schedules.size());
  }

  @Test
  public void testFlowNameFilter() throws Exception {
    List<Schedule> schedules = manager.getSchedules("", "A",
        "", -1, -1);

    Assert.assertEquals(2, schedules.size());

    schedules = manager.getSchedules("", "a",
        "", -1, -1);

    Assert.assertEquals(2, schedules.size());
  }

  @Test
  public void testSubmitUserFilter() throws Exception {
    List<Schedule> schedules = manager.getSchedules("", "",
        "azkaban", -1, -1);

    Assert.assertEquals(2, schedules.size());

    schedules = manager.getSchedules("", "",
        "AZKABAN", -1, -1);

    Assert.assertEquals(2, schedules.size());
  }

  @Test
  public void testAllFilters() throws Exception {
    List<Schedule> schedules = manager.getSchedules("v1", "flow",
        "azka", DateTime.now().getMillis() - 10000,
        DateTime.now().getMillis() + 10000);

    Assert.assertEquals(2, schedules.size());
  }
}

/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.trigger;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import azkaban.utils.Utils;
import azkaban.trigger.builtin.BasicTimeChecker;

public class BasicTimeCheckerTest {


  private Condition getCondition(BasicTimeChecker timeChecker){
    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();
    checkers.put(timeChecker.getId(), timeChecker);
    String expr = timeChecker.getId() + ".eval()";

    return new Condition(checkers, expr);
  }

  @Test
  public void periodTimerTest() {

    // get a new timechecker, start from now, repeat every minute. should
    // evaluate to false now, and true a minute later.
    DateTime now = DateTime.now();
    ReadablePeriod period = Utils.parsePeriodString("10s");

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            now.getZone(), true, true, period, null);

    Condition cond = getCondition(timeChecker);

    assertFalse(cond.isMet());

    // sleep for 1 min
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    assertTrue(cond.isMet());
    cond.resetCheckers();
    assertFalse(cond.isMet());

    // sleep for 1 min
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    assertTrue(cond.isMet());
  }

  /**
   * Test Base Cron Functionality.
   */
  @Test
  public void testQuartzCurrentZone() {

    DateTime now = DateTime.now();
    String cronExpression = "0 0 0 31 12 ? 2050";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            now.getZone(), true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    Condition cond = getCondition(timeChecker);
    // 2556086400000L represent for "2050-12-31T00:00:00.000-08:00"

    DateTime year2050 = new DateTime(2050, 12, 31, 0 ,0 ,0 ,now.getZone());
    assertTrue(cond.getNextCheckTime() == year2050.getMillis());
  }

  /**
   * Test when PST-->PDT happens in 2020. -8:00 -> -7:00
   * See details why confusion happens during this change: https://en.wikipedia.org/wiki/Pacific_Time_Zone
   *
   * This test demonstrates that if the cron is under UTC settings,
   * When daylight saving change occurs, 2:30 will be changed to 3:30 at that day.
   */
  @Test
  public void testPSTtoPDTunderUTC() {

    DateTime now = DateTime.now();

    // 10:30 UTC == 2:30 PST
    String cronExpression = "0 30 10 8 3 ? 2020";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            DateTimeZone.UTC, true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    Condition cond = getCondition(timeChecker);

    DateTime spring2020UTC = new DateTime(2020, 3, 8, 10, 30, 0, DateTimeZone.UTC);
    DateTime spring2020PDT = new DateTime(2020, 3, 8, 3, 30, 0, DateTimeZone.forID("America/Los_Angeles"));
    assertTrue(cond.getNextCheckTime() == spring2020UTC.getMillis());
    assertTrue(cond.getNextCheckTime() == spring2020PDT.getMillis());
  }

  /**
   * Test when PST-->PDT happens in 2020. -8:00 -> -7:00
   * See details why confusion happens during this change: https://en.wikipedia.org/wiki/Pacific_Time_Zone
   *
   * This test demonstrates that 2:30 AM will not happen during the daylight saving day on Cron settings under PDT/PST.
   * Since we let the cron triggered both at March 8th, and 9th, it will execute at March 9th.
   */
  @Test
  public void testPSTtoPDTdst2() {

    DateTime now = DateTime.now();

    String cronExpression = "0 30 2 8,9 3 ? 2020";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecker_1", now.getMillis(),
            DateTimeZone.forID("America/Los_Angeles"), true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    Condition cond = getCondition(timeChecker);

    DateTime aTime = new DateTime(2020, 3, 9, 2, 30, 0, DateTimeZone.forID("America/Los_Angeles"));
    assertTrue(cond.getNextCheckTime() == aTime.getMillis());
  }

  /**
   * Test when PDT-->PST happens in 2020. -7:00 -> -8:00
   * See details why confusion happens during this change: https://en.wikipedia.org/wiki/Pacific_Time_Zone
   *
   * This test cronDayLightPacificWinter1 is in order to compare against the cronDayLightPacificWinter2.
   *
   * In this Test, we let job run at 1:00 at Nov.1st, 2020. We know that we will have two 1:00 at that day.
   * The test shows that the first 1:00 is skipped at that day.
   * Schedule will still be executed once on that day.
   */
  @Test
  public void testPDTtoPSTdst1() {

    DateTime now = DateTime.now();

    // 9:00 UTC == 1:00 PST (difference is 8 hours)
    String cronExpression = "0 0 1 1,2 11 ? 2020";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            DateTimeZone.forID("America/Los_Angeles"), true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    Condition cond = getCondition(timeChecker);

    DateTime winter2020 = new DateTime(2020, 11, 1, 9, 0, 0, DateTimeZone.UTC);

    DateTime winter2020_2 = new DateTime(2020, 11, 1, 1, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    DateTime winter2020_3 = new DateTime(2020, 11, 1, 2, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    assertTrue(cond.getNextCheckTime() == winter2020.getMillis());


    // Both 1 and 2 o'clock can not pass the test. Based on milliseconds we got,
    // winter2020_2.getMillis() == 11/1/2020, 1:00:00 AM GMT-7:00 DST
    // winter2020_3.getMillis() == 11/1/2020, 2:00:00 AM GMT-8:00
    // Both time doesn't match the second 1:00 AM
    assertFalse(cond.getNextCheckTime() == winter2020_2.getMillis());
    assertFalse(cond.getNextCheckTime() == winter2020_3.getMillis());
  }


  /**
   * Test when PDT-->PST happens in 2020. -7:00 -> -8:00
   * See details why confusion happens during this change: https://en.wikipedia.org/wiki/Pacific_Time_Zone
   *
   * This test cronDayLightPacificWinter2 is in order to be compared against the cronDayLightPacificWinter1.
   *
   * In this Test, we let job run at 0:59 at Nov.1st, 2020. it shows that it is 7:59 UTC
   * The test shows 7:59 UTC jump to 9:00 UTC.
   */
  @Test
  public void testPDTtoPSTdst2() {

    DateTime now = DateTime.now();

    // 7:59 UTC == 0:59 PDT (difference is 7 hours)
    String cronExpression = "0 59 0 1,2 11 ? 2020";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            DateTimeZone.forID("America/Los_Angeles"), true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    Condition cond = getCondition(timeChecker);

    // 7:59 UTC == 0:59 PDT (difference is 7 hours)
    DateTime winter2020 = new DateTime(2020, 11, 1, 7, 59, 0, DateTimeZone.UTC);
    DateTime winter2020_2 = new DateTime(2020, 11, 1, 0, 59, 0, DateTimeZone.forID("America/Los_Angeles"));

    // Local time remains the same.
    assertTrue(cond.getNextCheckTime() == winter2020.getMillis());
    assertTrue(cond.getNextCheckTime() == winter2020_2.getMillis());
  }



  /**
   * Test when PDT-->PST happens in 2020. -7:00 -> -8:00
   * See details why confusion happens during this change: https://en.wikipedia.org/wiki/Pacific_Time_Zone
   *
   * This test is a supplement to cronDayLightPacificWinter1.
   *
   * Still, we let job run at 1:30 at Nov.1st, 2020. We know that we will have two 1:30 at that day.
   * The test shows the 1:30 at that day will be based on PST, not PDT. It means that the first 1:30 is skipped at that day.
   */
  @Test
  public void testPDTtoPSTdst3() {
    
    DateTime now = DateTime.now();

    // 9:30 UTC == 1:30 PST (difference is 8 hours)
    String cronExpression = "0 30 1 1,2 11 ? 2020";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            DateTimeZone.forID("America/Los_Angeles"), true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    Condition cond = getCondition(timeChecker);

    // 9:30 UTC == 1:30 PST (difference is 8 hours)
    DateTime winter2020 = new DateTime(2020, 11, 1, 9, 30, 0, DateTimeZone.UTC);

    DateTime winter2020_2 = new DateTime(2020, 11, 1, 1, 30, 0, DateTimeZone.forID("America/Los_Angeles"));
    DateTime winter2020_3 = new DateTime(2020, 11, 1, 2, 30, 0, DateTimeZone.forID("America/Los_Angeles"));
    assertTrue(cond.getNextCheckTime() == winter2020.getMillis());

    // Both 1:30 and 2:30 can not pass the test.
    assertFalse(cond.getNextCheckTime() == winter2020_2.getMillis());
    assertFalse(cond.getNextCheckTime() == winter2020_3.getMillis());
  }
}

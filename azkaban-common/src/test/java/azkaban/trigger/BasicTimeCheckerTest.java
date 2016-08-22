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

  @Test
  public void periodTimerTest() {

    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();

    // get a new timechecker, start from now, repeat every minute. should
    // evaluate to false now, and true a minute later.
    DateTime now = DateTime.now();
    ReadablePeriod period = Utils.parsePeriodString("10s");

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            now.getZone(), true, true, period, null);
    checkers.put(timeChecker.getId(), timeChecker);
    String expr = timeChecker.getId() + ".eval()";

    Condition cond = new Condition(checkers, expr);
    System.out.println(expr);

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
  public void cronTimerTest1() {

    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();

    DateTime now = DateTime.now();
    String cronExpression = "0 0 0 31 12 ? 2050";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            now.getZone(), true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    checkers.put(timeChecker.getId(), timeChecker);
    String expr = timeChecker.getId() + ".eval()";
    Condition cond = new Condition(checkers, expr);
    // 2556086400000L represent for "2050-12-31T00:00:00.000-08:00"

    DateTime year2050 = new DateTime(2050, 12, 31, 0 ,0 ,0 ,now.getZone());
    assertTrue(cond.getNextCheckTime() == year2050.getMillis());
  }

  /**
   * Test when PST-->PDT happens in 2020. -8:00 -> -7:00
   * See details why confusion happens during this change: https://en.wikipedia.org/wiki/Pacific_Time_Zone
   */
  @Test
  public void cronDayLightChangeTestSucceed() {

    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();

    DateTime now = DateTime.now();
    // 10:30 UTC == 2:30 PST
    String cronExpression = "0 30 10 8 3 ? 2020";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            DateTimeZone.UTC, true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    checkers.put(timeChecker.getId(), timeChecker);
    String expr = timeChecker.getId() + ".eval()";
    Condition cond = new Condition(checkers, expr);

    DateTime spring2020 = new DateTime(2020, 3, 8, 10, 30, 0, DateTimeZone.UTC);
    assertTrue(cond.getNextCheckTime() == spring2020.getMillis());
  }

  /**
   * Test when PST-->PDT happens in 2020. -8:00 -> -7:00
   * See details why confusion happens during this change: https://en.wikipedia.org/wiki/Pacific_Time_Zone
   *
   * This test demonstrates that 2:30 AM will not happen on Cron settings under PDT/PST.
   */
  @Test(expected=IllegalStateException.class)
  public void cronDayLightChangeTestFailure() {

    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();

    DateTime now = DateTime.now();

    String cronExpression = "0 30 2 8 3 ? 2020";

    // Since no 2:30 PST in March 8th in that day, IllegalStateException will always throw.
    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            now.getZone(), true, true, null, cronExpression);
  }

  /**
   * Test when PDT-->PST happens in 2020. -7:00 -> -8:00
   * See details why confusion happens during this change: https://en.wikipedia.org/wiki/Pacific_Time_Zone
   */
  @Test
  public void cronDayLightChangeSucceed2() {

    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();

    DateTime now = DateTime.now();

    // 8:30 UTC == 1:30 PDT
    String cronExpression = "0 30 8 1 11 ? 2020";

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            DateTimeZone.UTC, true, true, null, cronExpression);
    System.out.println("getNextCheckTime = " + timeChecker.getNextCheckTime());

    checkers.put(timeChecker.getId(), timeChecker);
    String expr = timeChecker.getId() + ".eval()";
    Condition cond = new Condition(checkers, expr);

    DateTime winter2020 = new DateTime(2020, 11, 1, 8, 30, 0, DateTimeZone.UTC);
    assertTrue(cond.getNextCheckTime() == winter2020.getMillis());
  }
}

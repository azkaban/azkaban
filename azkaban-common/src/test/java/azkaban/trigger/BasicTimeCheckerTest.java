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
import org.joda.time.ReadablePeriod;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import azkaban.utils.Utils;
import azkaban.trigger.builtin.BasicTimeChecker;

public class BasicTimeCheckerTest {

  @Test
  public void basicTimerTest() {

    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();

    // get a new timechecker, start from now, repeat every minute. should
    // evaluate to false now, and true a minute later.
    DateTime now = DateTime.now();
    ReadablePeriod period = Utils.parsePeriodString("10s");

    BasicTimeChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecket_1", now.getMillis(),
            now.getZone(), true, true, period);
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
}

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

package azkaban.trigger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import azkaban.trigger.builtin.EndTimeChecker;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class EndTimeCheckerTest {

  private Condition getCondition(EndTimeChecker timeChecker){
    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();
    checkers.put(timeChecker.getId(), timeChecker);
    String expr = timeChecker.getId() + ".eval()";

    return new Condition(checkers, expr);
  }

  @Test
  public void expireTest() {

    long nowPlus5seconds = System.currentTimeMillis() + 2000L;
    EndTimeChecker timeChecker = new EndTimeChecker("expire", nowPlus5seconds);
    Condition cond = getCondition(timeChecker);

    assertFalse(cond.isMet());
    // sleep for 4s
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue(cond.isMet());
    timeChecker.stopChecker();
    assertFalse(cond.isMet());
  }

  @Test
  public void neverExpireTest() {

    EndTimeChecker timeChecker = new EndTimeChecker("expire", -1);
    Condition cond = getCondition(timeChecker);

    assertFalse(cond.isMet());
    // sleep for 1s
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertFalse(cond.isMet());
  }
}

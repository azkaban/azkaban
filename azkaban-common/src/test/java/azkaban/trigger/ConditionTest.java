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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.TimeUtils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;

public class ConditionTest {

  @Test
  public void conditionTest() {

    final Map<String, ConditionChecker> checkers =
        new HashMap<>();

    final ThresholdChecker fake1 = new ThresholdChecker("thresholdchecker1", 10);
    final ThresholdChecker fake2 = new ThresholdChecker("thresholdchecker2", 20);
    ThresholdChecker.setVal(15);
    checkers.put(fake1.getId(), fake1);
    checkers.put(fake2.getId(), fake2);

    final String expr1 =
        "( " + fake1.getId() + ".eval()" + " && " + fake2.getId() + ".eval()"
            + " )" + " || " + "( " + fake1.getId() + ".eval()" + " && " + "!"
            + fake2.getId() + ".eval()" + " )";
    final String expr2 =
        "( " + fake1.getId() + ".eval()" + " && " + fake2.getId() + ".eval()"
            + " )" + " || " + "( " + fake1.getId() + ".eval()" + " && "
            + fake2.getId() + ".eval()" + " )";

    final Condition cond = new Condition(checkers, expr1);

    System.out.println("Setting expression " + expr1);
    assertTrue(cond.isMet());
    cond.setExpression(expr2);
    System.out.println("Setting expression " + expr2);
    assertFalse(cond.isMet());

  }

  @Ignore
  @Test
  public void jsonConversionTest() throws Exception {

    final CheckerTypeLoader checkerTypeLoader = new CheckerTypeLoader();
    checkerTypeLoader.init(new Props());
    Condition.setCheckerLoader(checkerTypeLoader);

    final Map<String, ConditionChecker> checkers =
        new HashMap<>();

    // get a new timechecker, start from now, repeat every minute. should
    // evaluate to false now, and true a minute later.
    final DateTime now = DateTime.now();
    final String period = "6s";

    // BasicTimeChecker timeChecker = new BasicTimeChecker(now, true, true,
    // period);
    final ConditionChecker timeChecker =
        new BasicTimeChecker("BasicTimeChecker_1", now.getMillis(),
            now.getZone(), true, true, TimeUtils.parsePeriodString(period), null);
    System.out.println("checker id is " + timeChecker.getId());

    checkers.put(timeChecker.getId(), timeChecker);
    final String expr = timeChecker.getId() + ".eval()";

    final Condition cond = new Condition(checkers, expr);

    final File temp = File.createTempFile("temptest", "temptest");
    temp.deleteOnExit();
    final Object obj = cond.toJson();
    JSONUtils.toJSON(obj, temp);

    final Condition cond2 = Condition.fromJson(JSONUtils.parseJSONFromFile(temp));

    final Map<String, ConditionChecker> checkers2 = cond2.getCheckers();

    assertTrue(cond.getExpression().equals(cond2.getExpression()));
    System.out.println("cond1: " + cond.getExpression());
    System.out.println("cond2: " + cond2.getExpression());
    assertTrue(checkers2.size() == 1);
    final ConditionChecker checker2 = checkers2.get(timeChecker.getId());
    // assertTrue(checker2.getId().equals(timeChecker.getId()));
    System.out.println("checker1: " + timeChecker.getId());
    System.out.println("checker2: " + checker2.getId());
    assertTrue(timeChecker.getId().equals(checker2.getId()));
  }

}

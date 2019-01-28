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

import static org.junit.Assert.assertTrue;

import azkaban.executor.ExecutionOptions;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TriggerTest {

  private CheckerTypeLoader checkerLoader;
  private ActionTypeLoader actionLoader;

  @Before
  public void setup() throws TriggerException {
    this.checkerLoader = new CheckerTypeLoader();
    this.checkerLoader.init(new Props());
    Condition.setCheckerLoader(this.checkerLoader);
    this.actionLoader = new ActionTypeLoader();
    this.actionLoader.init(new Props());
    Trigger.setActionTypeLoader(this.actionLoader);
  }

  @Ignore
  @Test
  public void jsonConversionTest() throws Exception {
    final DateTime now = DateTime.now();
    final ConditionChecker checker1 =
        new BasicTimeChecker("timeChecker1", now.getMillis(), now.getZone(),
            true, true, Utils.parsePeriodString("1h"), null);
    final Map<String, ConditionChecker> checkers1 =
        new HashMap<>();
    checkers1.put(checker1.getId(), checker1);
    final String expr1 = checker1.getId() + ".eval()";
    final Condition triggerCond = new Condition(checkers1, expr1);
    final Condition expireCond = new Condition(checkers1, expr1);
    final List<TriggerAction> actions = new ArrayList<>();
    final TriggerAction action =
        new ExecuteFlowAction("executeAction", 1, "testProj", "testFlow",
            "azkaban", new ExecutionOptions());
    actions.add(action);

    final Trigger t = new Trigger.TriggerBuilder("azkaban",
        "test",
        triggerCond,
        expireCond,
        actions).build();

    final File temp = File.createTempFile("temptest", "temptest");
    temp.deleteOnExit();
    final Object obj = t.toJson();
    JSONUtils.toJSON(obj, temp);

    final Trigger t2 = Trigger.fromJson(JSONUtils.parseJSONFromFile(temp));

    assertTrue(t.getSource().equals(t2.getSource()));
    assertTrue(t.getTriggerId() == t2.getTriggerId());

  }

}

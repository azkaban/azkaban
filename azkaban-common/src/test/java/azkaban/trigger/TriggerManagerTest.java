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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import azkaban.utils.Props;

public class TriggerManagerTest {

  private TriggerLoader triggerLoader;

  @Before
  public void setup() throws TriggerException, TriggerManagerException {
    triggerLoader = new MockTriggerLoader();

  }

  @After
  public void tearDown() {

  }

  @Ignore @Test
  public void triggerManagerSimpleTest() throws TriggerManagerException {
    Props props = new Props();
    props.put("trigger.scan.interval", 4000);
    TriggerManager triggerManager =
        new TriggerManager(props, triggerLoader, null);

    triggerManager.registerCheckerType(ThresholdChecker.type,
        ThresholdChecker.class);
    triggerManager.registerActionType(DummyTriggerAction.type,
        DummyTriggerAction.class);

    ThresholdChecker.setVal(1);

    triggerManager.insertTrigger(
        createDummyTrigger("test1", "triggerLoader", 10), "testUser");
    List<Trigger> triggers = triggerManager.getTriggers();
    assertTrue(triggers.size() == 1);
    Trigger t1 = triggers.get(0);
    t1.setResetOnTrigger(false);
    triggerManager.updateTrigger(t1, "testUser");
    ThresholdChecker checker1 =
        (ThresholdChecker) t1.getTriggerCondition().getCheckers().values()
            .toArray()[0];
    assertTrue(t1.getSource().equals("triggerLoader"));

    Trigger t2 =
        createDummyTrigger("test2: add new trigger", "addNewTriggerTest", 20);
    triggerManager.insertTrigger(t2, "testUser");
    ThresholdChecker checker2 =
        (ThresholdChecker) t2.getTriggerCondition().getCheckers().values()
            .toArray()[0];

    ThresholdChecker.setVal(15);
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    assertTrue(checker1.isCheckerMet() == false);
    assertTrue(checker2.isCheckerMet() == false);
    assertTrue(checker1.isCheckerReset() == false);
    assertTrue(checker2.isCheckerReset() == false);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    assertTrue(checker1.isCheckerMet() == true);
    assertTrue(checker2.isCheckerMet() == false);
    assertTrue(checker1.isCheckerReset() == false);
    assertTrue(checker2.isCheckerReset() == false);

    ThresholdChecker.setVal(25);
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    assertTrue(checker1.isCheckerMet() == true);
    assertTrue(checker1.isCheckerReset() == false);
    assertTrue(checker2.isCheckerReset() == true);

    triggers = triggerManager.getTriggers();
    assertTrue(triggers.size() == 1);

  }

  public class MockTriggerLoader implements TriggerLoader {

    private Map<Integer, Trigger> triggers = new HashMap<Integer, Trigger>();
    private int idIndex = 0;

    @Override
    public void addTrigger(Trigger t) throws TriggerLoaderException {
      t.setTriggerId(idIndex++);
      triggers.put(t.getTriggerId(), t);
    }

    @Override
    public void removeTrigger(Trigger s) throws TriggerLoaderException {
      triggers.remove(s.getTriggerId());

    }

    @Override
    public void updateTrigger(Trigger t) throws TriggerLoaderException {
      triggers.put(t.getTriggerId(), t);
    }

    @Override
    public List<Trigger> loadTriggers() {
      return new ArrayList<Trigger>(triggers.values());
    }

    @Override
    public Trigger loadTrigger(int triggerId) throws TriggerLoaderException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<Trigger> getUpdatedTriggers(long lastUpdateTime)
        throws TriggerLoaderException {
      // TODO Auto-generated method stub
      return null;
    }

  }

  private Trigger createDummyTrigger(String message, String source,
      int threshold) {

    Map<String, ConditionChecker> checkers =
        new HashMap<String, ConditionChecker>();
    ConditionChecker checker =
        new ThresholdChecker(ThresholdChecker.type, threshold);
    checkers.put(checker.getId(), checker);

    List<TriggerAction> actions = new ArrayList<TriggerAction>();
    TriggerAction act = new DummyTriggerAction(message);
    actions.add(act);

    String expr = checker.getId() + ".eval()";

    Condition triggerCond = new Condition(checkers, expr);
    Condition expireCond = new Condition(checkers, expr);

    Trigger fakeTrigger =
        new Trigger(DateTime.now().getMillis(), DateTime.now().getMillis(),
            "azkaban", source, triggerCond, expireCond, actions);
    fakeTrigger.setResetOnTrigger(true);
    fakeTrigger.setResetOnExpire(true);

    return fakeTrigger;
  }

  // public class MockCheckerLoader extends CheckerTypeLoader{
  //
  // @Override
  // public void init(Props props) {
  // checkerToClass.put(ThresholdChecker.type, ThresholdChecker.class);
  // }
  // }
  //
  // public class MockActionLoader extends ActionTypeLoader {
  // @Override
  // public void init(Props props) {
  // actionToClass.put(DummyTriggerAction.type, DummyTriggerAction.class);
  // }
  // }

}

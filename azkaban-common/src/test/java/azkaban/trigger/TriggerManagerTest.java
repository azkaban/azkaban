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

import azkaban.utils.Props;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TriggerManagerTest {

  private TriggerLoader triggerLoader;

  @Before
  public void setup() throws TriggerException, TriggerManagerException {
    this.triggerLoader = new MockTriggerLoader();

  }

  @After
  public void tearDown() {

  }

  @Ignore
  @Test
  public void triggerManagerSimpleTest() throws TriggerManagerException {
    final Props props = new Props();
    props.put("trigger.scan.interval", 4000);
    final TriggerManager triggerManager =
        new TriggerManager(props, this.triggerLoader, null);

    triggerManager.registerCheckerType(ThresholdChecker.type,
        ThresholdChecker.class);
    triggerManager.registerActionType(DummyTriggerAction.type,
        DummyTriggerAction.class);

    ThresholdChecker.setVal(1);

    triggerManager.insertTrigger(
        createDummyTrigger("test1", "triggerLoader", 10), "testUser");
    List<Trigger> triggers = triggerManager.getTriggers();
    assertTrue(triggers.size() == 1);
    final Trigger t1 = triggers.get(0);
    t1.setResetOnTrigger(false);
    triggerManager.updateTrigger(t1, "testUser");
    final ThresholdChecker checker1 =
        (ThresholdChecker) t1.getTriggerCondition().getCheckers().values()
            .toArray()[0];
    assertTrue(t1.getSource().equals("triggerLoader"));

    final Trigger t2 =
        createDummyTrigger("test2: add new trigger", "addNewTriggerTest", 20);
    triggerManager.insertTrigger(t2, "testUser");
    final ThresholdChecker checker2 =
        (ThresholdChecker) t2.getTriggerCondition().getCheckers().values()
            .toArray()[0];

    ThresholdChecker.setVal(15);
    try {
      Thread.sleep(2000);
    } catch (final InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    assertTrue(checker1.isCheckerMet() == false);
    assertTrue(checker2.isCheckerMet() == false);
    assertTrue(checker1.isCheckerReset() == false);
    assertTrue(checker2.isCheckerReset() == false);

    try {
      Thread.sleep(2000);
    } catch (final InterruptedException e) {
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
    } catch (final InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    assertTrue(checker1.isCheckerMet() == true);
    assertTrue(checker1.isCheckerReset() == false);
    assertTrue(checker2.isCheckerReset() == true);

    triggers = triggerManager.getTriggers();
    assertTrue(triggers.size() == 1);

  }

  private Trigger createDummyTrigger(final String message, final String source,
      final int threshold) {

    final Map<String, ConditionChecker> checkers =
        new HashMap<>();
    final ConditionChecker checker =
        new ThresholdChecker(ThresholdChecker.type, threshold);
    checkers.put(checker.getId(), checker);

    final List<TriggerAction> actions = new ArrayList<>();
    final TriggerAction act = new DummyTriggerAction(message);
    actions.add(act);

    final String expr = checker.getId() + ".eval()";

    final Condition triggerCond = new Condition(checkers, expr);
    final Condition expireCond = new Condition(checkers, expr);

    final Trigger fakeTrigger = new Trigger.TriggerBuilder("azkaban",
        source,
        triggerCond,
        expireCond,
        actions).build();

    fakeTrigger.setResetOnTrigger(true);
    fakeTrigger.setResetOnExpire(true);

    return fakeTrigger;
  }

  public static class MockTriggerLoader implements TriggerLoader {

    private final Map<Integer, Trigger> triggers = new HashMap<>();
    private int idIndex = 0;

    @Override
    public void addTrigger(final Trigger t) throws TriggerLoaderException {
      t.setTriggerId(this.idIndex++);
      this.triggers.put(t.getTriggerId(), t);
    }

    @Override
    public void removeTrigger(final Trigger s) throws TriggerLoaderException {
      this.triggers.remove(s.getTriggerId());

    }

    @Override
    public void updateTrigger(final Trigger t) throws TriggerLoaderException {
      this.triggers.put(t.getTriggerId(), t);
    }

    @Override
    public List<Trigger> loadTriggers() {
      return new ArrayList<>(this.triggers.values());
    }

    @Override
    public Trigger loadTrigger(final int triggerId) throws TriggerLoaderException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<Trigger> getUpdatedTriggers(final long lastUpdateTime)
        throws TriggerLoaderException {
      // TODO Auto-generated method stub
      return null;
    }

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

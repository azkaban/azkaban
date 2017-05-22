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

import azkaban.executor.ExecutorManager;

import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import azkaban.utils.Props;

import static org.mockito.Mockito.*;

public class TriggerManagerTest {

  private static TriggerLoader triggerLoader;
  private static ExecutorManager executorManager;
  private TriggerManager triggerManager;

  @BeforeClass
  public static void prepare() {
    triggerLoader = new MockTriggerLoader();
    executorManager = mock(ExecutorManager.class);
    doNothing().when(executorManager).addListener(anyObject());
  }

  @Before
  public void setup() throws TriggerException, TriggerManagerException {
    Props props = new Props();
    props.put("trigger.scan.interval", 300);
    triggerManager = new TriggerManager(props, triggerLoader, executorManager);
    triggerManager.registerCheckerType(ThresholdChecker.type,
        ThresholdChecker.class);
    triggerManager.registerActionType(DummyTriggerAction.type,
        DummyTriggerAction.class);
    triggerManager.start();
  }

  @After
  public void tearDown() {
    triggerManager.shutdown();
  }

  @Test
  public void neverExpireTriggerTest() throws TriggerManagerException {

    Trigger t1 = createNeverExpireTrigger("triggerLoader", 10);
    triggerManager.insertTrigger(t1);
    t1.setResetOnTrigger(false);
    ThresholdChecker triggerChecker =
        (ThresholdChecker) t1.getTriggerCondition().getCheckers().values()
            .toArray()[0];

    BasicTimeChecker expireChecker =
        (BasicTimeChecker) t1.getExpireCondition().getCheckers().values()
            .toArray()[0];

    ThresholdChecker.setVal(15);
    sleep(300);
    assertTrue(triggerChecker.isCheckerMet() == true);
    assertTrue(expireChecker.eval() == false);

    ThresholdChecker.setVal(25);
    sleep(300);
    assertTrue(triggerChecker.isCheckerMet() == true);
    assertTrue(expireChecker.eval() == false);
  }


  @Test
  public void timeCheckerAndExpireTriggerTest() throws TriggerManagerException {

    long curr = System.currentTimeMillis();
    Trigger t1 = createPeriodAndEndCheckerTrigger(curr);
    triggerManager.insertTrigger(t1);
    t1.setResetOnTrigger(true);
    BasicTimeChecker expireChecker =
        (BasicTimeChecker) t1.getExpireCondition().getCheckers().values()
            .toArray()[0];

    sleep(1000);

    assertTrue(expireChecker.eval() == false);
    assertTrue(t1.getStatus() == TriggerStatus.READY);

    sleep(1000);
    sleep(1000);
    sleep(1000);
    assertTrue(expireChecker.eval() == true);
    assertTrue(t1.getStatus() == TriggerStatus.PAUSED);

    sleep(1000);
    assertTrue(expireChecker.eval() == true);
    assertTrue(t1.getStatus() == TriggerStatus.PAUSED);
  }


  public static class MockTriggerLoader implements TriggerLoader {
    private Map<Integer, Trigger> triggers = new HashMap<>();
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
      return new ArrayList<>(triggers.values());
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

  private void sleep (long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Trigger createDummyTrigger(String source, int threshold) {

    Map<String, ConditionChecker> checkers = new HashMap<>();
    ConditionChecker checker = new ThresholdChecker(ThresholdChecker.type, threshold);
    checkers.put(checker.getId(), checker);

    String expr = checker.getId() + ".eval()";

    Condition triggerCond = new Condition(checkers, expr);
    Condition expireCond = new Condition(checkers, expr);

    Trigger fakeTrigger = new Trigger.TriggerBuilder("azkaban",
        source,
        triggerCond,
        expireCond,
        getTriggerActions()).build();

    fakeTrigger.setResetOnTrigger(true);
    fakeTrigger.setResetOnExpire(true);

    return fakeTrigger;
  }


  private Trigger createNeverExpireTrigger(String source, int threshold) {
    Map<String, ConditionChecker> triggerCheckers = new HashMap<>();
    Map<String, ConditionChecker> expireCheckers = new HashMap<>();
    ConditionChecker triggerChecker = new ThresholdChecker(ThresholdChecker.type, threshold);
    ConditionChecker endTimeChecker = new BasicTimeChecker("EndTimeCheck_1", 111L,
        DateTimeZone.UTC, 2536871155000L,false, false,
        null, null);
    triggerCheckers.put(triggerChecker.getId(), triggerChecker);
    expireCheckers.put(endTimeChecker.getId(), endTimeChecker);

    String triggerExpr = triggerChecker.getId() + ".eval()";
    String expireExpr = endTimeChecker.getId() + ".eval()";

    Condition triggerCond = new Condition(triggerCheckers, triggerExpr);
    Condition expireCond = new Condition(expireCheckers, expireExpr);

    Trigger fakeTrigger = new Trigger.TriggerBuilder("azkaban",
        source,
        triggerCond,
        expireCond,
        getTriggerActions()).build();

    fakeTrigger.setResetOnTrigger(false);
    fakeTrigger.setResetOnExpire(true);
    return fakeTrigger;
  }

  private Trigger createPeriodAndEndCheckerTrigger(long currMillis) {
    Map<String, ConditionChecker> triggerCheckers = new HashMap<>();
    Map<String, ConditionChecker> expireCheckers = new HashMap<>();

    // TODO kunkun-tang: 1 second is the minimum unit for {@link org.joda.time.ReadablePeriod}.
    // In future, we should use some smaller alternative.
    ConditionChecker triggerChecker = new BasicTimeChecker("BasicTimeChecker_1",
        currMillis, DateTimeZone.UTC, true, true,
        Utils.parsePeriodString("1s"), null);

    // End time is 3 seconds past now.
    ConditionChecker endTimeChecker = new BasicTimeChecker("EndTimeCheck_1", 111L,
        DateTimeZone.UTC, currMillis + 3000L,false, false,
        null, null);
    triggerCheckers.put(triggerChecker.getId(), triggerChecker);
    expireCheckers.put(endTimeChecker.getId(), endTimeChecker);

    String triggerExpr = triggerChecker.getId() + ".eval()";
    String expireExpr = endTimeChecker.getId() + ".eval()";

    Condition triggerCond = new Condition(triggerCheckers, triggerExpr);
    Condition expireCond = new Condition(expireCheckers, expireExpr);

    Trigger timeTrigger = new Trigger.TriggerBuilder("azkaban",
        "",
        triggerCond,
        expireCond,
        getTriggerActions()).build();

    timeTrigger.setResetOnTrigger(false);
    timeTrigger.setResetOnExpire(true);
    return timeTrigger;
  }

  private List<TriggerAction> getTriggerActions() {
    List<TriggerAction> actions = new ArrayList<>();
    TriggerAction act = new DummyTriggerAction("");
    actions.add(act);
    return actions;
  }
}

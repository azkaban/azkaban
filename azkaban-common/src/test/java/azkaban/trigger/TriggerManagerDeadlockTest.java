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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.alert.Alerter;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.MockExecutorLoader;
import azkaban.trigger.builtin.CreateTriggerAction;
import azkaban.utils.Props;

public class TriggerManagerDeadlockTest {

  TriggerLoader loader;
  TriggerManager triggerManager;
  ExecutorLoader execLoader;

  @Before
  public void setup() throws ExecutorManagerException, TriggerManagerException {
    loader = new MockTriggerLoader();
    Props props = new Props();
    props.put("trigger.scan.interval", 1000);
    props.put("executor.port", 12321);
    execLoader = new MockExecutorLoader();
    Map<String, Alerter> alerters = new HashMap<String, Alerter>();
    ExecutorManager executorManager =
        new ExecutorManager(props, execLoader, alerters);
    triggerManager = new TriggerManager(props, loader, executorManager);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void deadlockTest() throws TriggerLoaderException,
      TriggerManagerException {
    // this should well saturate it
    for (int i = 0; i < 1000; i++) {
      Trigger t = createSelfRegenTrigger();
      loader.addTrigger(t);
    }
    // keep going and add more
    for (int i = 0; i < 10000; i++) {
      Trigger d = createDummyTrigger();
      triggerManager.insertTrigger(d);
      triggerManager.removeTrigger(d);
    }

    System.out.println("No dead lock.");
  }

  public class AlwaysOnChecker implements ConditionChecker {

    public static final String type = "AlwaysOnChecker";

    private final String id;
    private final Boolean alwaysOn;

    public AlwaysOnChecker(String id, Boolean alwaysOn) {
      this.id = id;
      this.alwaysOn = alwaysOn;
    }

    @Override
    public Object eval() {
      // TODO Auto-generated method stub
      return alwaysOn;
    }

    @Override
    public Object getNum() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub

    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getType() {
      // TODO Auto-generated method stub
      return type;
    }

    @Override
    public ConditionChecker fromJson(Object obj) throws Exception {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object toJson() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void stopChecker() {
      // TODO Auto-generated method stub

    }

    @Override
    public void setContext(Map<String, Object> context) {
      // TODO Auto-generated method stub

    }

    @Override
    public long getNextCheckTime() {
      // TODO Auto-generated method stub
      return 0;
    }

  }

  private Trigger createSelfRegenTrigger() {
    ConditionChecker alwaysOnChecker =
        new AlwaysOnChecker("alwaysOn", Boolean.TRUE);
    String triggerExpr = alwaysOnChecker.getId() + ".eval()";
    Map<String, ConditionChecker> triggerCheckers =
        new HashMap<String, ConditionChecker>();
    triggerCheckers.put(alwaysOnChecker.getId(), alwaysOnChecker);
    Condition triggerCond = new Condition(triggerCheckers, triggerExpr);

    TriggerAction triggerAct =
        new CreateTriggerAction("dummyTrigger", createDummyTrigger());
    List<TriggerAction> actions = new ArrayList<TriggerAction>();
    actions.add(triggerAct);

    ConditionChecker alwaysOffChecker =
        new AlwaysOnChecker("alwaysOff", Boolean.FALSE);
    String expireExpr = alwaysOffChecker.getId() + ".eval()";
    Map<String, ConditionChecker> expireCheckers =
        new HashMap<String, ConditionChecker>();
    expireCheckers.put(alwaysOffChecker.getId(), alwaysOffChecker);
    Condition expireCond = new Condition(expireCheckers, expireExpr);

    Trigger t =
        new Trigger("azkaban", "azkabanTest", triggerCond, expireCond, actions);
    return t;
  }

  private Trigger createDummyTrigger() {
    ConditionChecker alwaysOnChecker =
        new AlwaysOnChecker("alwaysOn", Boolean.TRUE);
    String triggerExpr = alwaysOnChecker.getId() + ".eval()";
    Map<String, ConditionChecker> triggerCheckers =
        new HashMap<String, ConditionChecker>();
    triggerCheckers.put(alwaysOnChecker.getId(), alwaysOnChecker);
    Condition triggerCond = new Condition(triggerCheckers, triggerExpr);

    TriggerAction triggerAct = new DummyTriggerAction("howdy!");
    List<TriggerAction> actions = new ArrayList<TriggerAction>();
    actions.add(triggerAct);

    ConditionChecker alwaysOffChecker =
        new AlwaysOnChecker("alwaysOff", Boolean.FALSE);
    String expireExpr = alwaysOffChecker.getId() + ".eval()";
    Map<String, ConditionChecker> expireCheckers =
        new HashMap<String, ConditionChecker>();
    expireCheckers.put(alwaysOffChecker.getId(), alwaysOffChecker);
    Condition expireCond = new Condition(expireCheckers, expireExpr);

    Trigger t =
        new Trigger("azkaban", "azkabanTest", triggerCond, expireCond, actions);
    return t;
  }

}

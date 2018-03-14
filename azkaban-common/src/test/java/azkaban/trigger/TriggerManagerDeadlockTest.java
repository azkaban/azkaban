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

import static org.mockito.Mockito.mock;

import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutorApiGateway;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.MockExecutorLoader;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.trigger.builtin.CreateTriggerAction;
import azkaban.utils.Props;
import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TriggerManagerDeadlockTest {

  TriggerLoader loader;
  TriggerManager triggerManager;
  ExecutorLoader execLoader;
  ExecutorApiGateway apiGateway;

  @Before
  public void setup() throws ExecutorManagerException, TriggerManagerException {
    this.loader = new MockTriggerLoader();
    final Props props = new Props();
    props.put("trigger.scan.interval", 1000);
    props.put("executor.port", 12321);
    this.execLoader = new MockExecutorLoader();
    this.apiGateway = mock(ExecutorApiGateway.class);
    final CommonMetrics commonMetrics = new CommonMetrics(new MetricsManager(new MetricRegistry()));
    final ExecutorManager executorManager = new ExecutorManager(props, this.execLoader,
        mock(AlerterHolder.class), commonMetrics, this.apiGateway);
    this.triggerManager = new TriggerManager(props, this.loader, executorManager);
  }

  @After
  public void tearDown() {

  }

  // TODO kunkun-tang: This test has problems. Will fix
  @Ignore
  @Test
  public void deadlockTest() throws TriggerLoaderException,
      TriggerManagerException {
    // this should well saturate it
    for (int i = 0; i < 1000; i++) {
      final Trigger t = createSelfRegenTrigger();
      this.loader.addTrigger(t);
    }
    // keep going and add more
    for (int i = 0; i < 10000; i++) {
      final Trigger d = createDummyTrigger();
      this.triggerManager.insertTrigger(d);
      this.triggerManager.removeTrigger(d);
    }

    System.out.println("No dead lock.");
  }

  private Trigger createSelfRegenTrigger() {
    final ConditionChecker alwaysOnChecker =
        new AlwaysOnChecker("alwaysOn", Boolean.TRUE);
    final String triggerExpr = alwaysOnChecker.getId() + ".eval()";
    final Map<String, ConditionChecker> triggerCheckers =
        new HashMap<>();
    triggerCheckers.put(alwaysOnChecker.getId(), alwaysOnChecker);
    final Condition triggerCond = new Condition(triggerCheckers, triggerExpr);

    final TriggerAction triggerAct =
        new CreateTriggerAction("dummyTrigger", createDummyTrigger());
    final List<TriggerAction> actions = new ArrayList<>();
    actions.add(triggerAct);

    final ConditionChecker alwaysOffChecker =
        new AlwaysOnChecker("alwaysOff", Boolean.FALSE);
    final String expireExpr = alwaysOffChecker.getId() + ".eval()";
    final Map<String, ConditionChecker> expireCheckers =
        new HashMap<>();
    expireCheckers.put(alwaysOffChecker.getId(), alwaysOffChecker);
    final Condition expireCond = new Condition(expireCheckers, expireExpr);

    final Trigger t =
        new Trigger.TriggerBuilder("azkaban",
            "azkabanTest",
            triggerCond,
            expireCond,
            actions).build();

    return t;
  }

  private Trigger createDummyTrigger() {
    final ConditionChecker alwaysOnChecker =
        new AlwaysOnChecker("alwaysOn", Boolean.TRUE);
    final String triggerExpr = alwaysOnChecker.getId() + ".eval()";
    final Map<String, ConditionChecker> triggerCheckers =
        new HashMap<>();
    triggerCheckers.put(alwaysOnChecker.getId(), alwaysOnChecker);
    final Condition triggerCond = new Condition(triggerCheckers, triggerExpr);

    final TriggerAction triggerAct = new DummyTriggerAction("howdy!");
    final List<TriggerAction> actions = new ArrayList<>();
    actions.add(triggerAct);

    final ConditionChecker alwaysOffChecker =
        new AlwaysOnChecker("alwaysOff", Boolean.FALSE);
    final String expireExpr = alwaysOffChecker.getId() + ".eval()";
    final Map<String, ConditionChecker> expireCheckers =
        new HashMap<>();
    expireCheckers.put(alwaysOffChecker.getId(), alwaysOffChecker);
    final Condition expireCond = new Condition(expireCheckers, expireExpr);

    final Trigger t =
        new Trigger.TriggerBuilder("azkaban",
            "azkabanTest",
            triggerCond,
            expireCond,
            actions).build();

    return t;
  }

  public static class AlwaysOnChecker implements ConditionChecker {

    public static final String type = "AlwaysOnChecker";

    private final String id;
    private final Boolean alwaysOn;

    public AlwaysOnChecker(final String id, final Boolean alwaysOn) {
      this.id = id;
      this.alwaysOn = alwaysOn;
    }

    @Override
    public Object eval() {
      // TODO Auto-generated method stub
      return this.alwaysOn;
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
      return this.id;
    }

    @Override
    public String getType() {
      // TODO Auto-generated method stub
      return type;
    }

    @Override
    public ConditionChecker fromJson(final Object obj) throws Exception {
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
    public void setContext(final Map<String, Object> context) {
      // TODO Auto-generated method stub

    }

    @Override
    public long getNextCheckTime() {
      // TODO Auto-generated method stub
      return 0;
    }

  }

}

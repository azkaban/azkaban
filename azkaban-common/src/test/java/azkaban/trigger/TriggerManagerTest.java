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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.metrics.MetricsManager;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.scheduler.MissedSchedulesManager;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Props;
import azkaban.utils.TimeUtils;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
// todo HappyRay: fix these slow tests or delete them.
public class TriggerManagerTest {

  private static TriggerLoader triggerLoader;
  private static ExecutorManagerAdapter executorManagerAdapter;
  private static ProjectManager projectManager;
  private TriggerManager triggerManager;
  private static MetricsManager metricsManager;

  @BeforeClass
  public static void prepare() {
    triggerLoader = new MockTriggerLoader();
    executorManagerAdapter = mock(ExecutorManagerAdapter.class);
    projectManager = mock(ProjectManager.class);
    metricsManager = mock(MetricsManager.class);
  }

  @Before
  public void setup() throws Exception {
    final Project project = new Project(1, "test-project");
    project.setFlows(ImmutableMap.of("test-flow", new Flow("test-flow")));
    when(projectManager.getProject(1)).thenReturn(project);
    when(executorManagerAdapter.submitExecutableFlow(any(), any()))
        .thenThrow(new ExecutorManagerException("Flow is already running. Skipping execution.",
            ExecutorManagerException.Reason.SkippedExecution));
    ExecuteFlowAction.setExecutorManager(this.executorManagerAdapter);
    ExecuteFlowAction.setProjectManager(this.projectManager);
    ExecuteFlowAction.setTriggerManager(this.triggerManager);
    final Props props = new Props();
    props.put("trigger.scan.interval", 300);
    MissedSchedulesManager missedScheduleManager = mock(MissedSchedulesManager.class);
    this.triggerManager = new TriggerManager(props, triggerLoader, executorManagerAdapter, metricsManager,
        missedScheduleManager);
    this.triggerManager.registerCheckerType(ThresholdChecker.type,
        ThresholdChecker.class);
    this.triggerManager.registerActionType(DummyTriggerAction.type,
        DummyTriggerAction.class);
    this.triggerManager.start();
  }

  @After
  public void tearDown() {
    this.triggerManager.shutdown();
  }

  @Test
  public void neverExpireTriggerTest() throws TriggerManagerException {

    final Trigger t1 = createNeverExpireTrigger("triggerLoader", 10);
    this.triggerManager.insertTrigger(t1);
    t1.setResetOnTrigger(false);
    final ThresholdChecker triggerChecker =
        (ThresholdChecker) t1.getTriggerCondition().getCheckers().values()
            .toArray()[0];

    final BasicTimeChecker expireChecker =
        (BasicTimeChecker) t1.getExpireCondition().getCheckers().values()
            .toArray()[0];

    ThresholdChecker.setVal(15);
    sleep(300);
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

    final long curr = System.currentTimeMillis();
    final Trigger t1 = createPeriodAndEndCheckerTrigger(curr);
    this.triggerManager.insertTrigger(t1);
    t1.setResetOnTrigger(true);
    final BasicTimeChecker expireChecker =
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

  private void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Trigger createNeverExpireTrigger(final String source, final int threshold) {
    final Map<String, ConditionChecker> triggerCheckers = new HashMap<>();
    final Map<String, ConditionChecker> expireCheckers = new HashMap<>();
    final ConditionChecker triggerChecker = new ThresholdChecker(ThresholdChecker.type, threshold);
    final ConditionChecker endTimeChecker = new BasicTimeChecker("EndTimeCheck_1", 111L,
        DateTimeZone.UTC, 2536871155000L, false, false,
        null, null);
    triggerCheckers.put(triggerChecker.getId(), triggerChecker);
    expireCheckers.put(endTimeChecker.getId(), endTimeChecker);

    final String triggerExpr = triggerChecker.getId() + ".eval()";
    final String expireExpr = endTimeChecker.getId() + ".eval()";

    final Condition triggerCond = new Condition(triggerCheckers, triggerExpr);
    final Condition expireCond = new Condition(expireCheckers, expireExpr);

    final Trigger fakeTrigger = new Trigger.TriggerBuilder("azkaban",
        source,
        triggerCond,
        expireCond,
        getTriggerActions()).build();

    fakeTrigger.setResetOnTrigger(false);
    fakeTrigger.setResetOnExpire(true);
    return fakeTrigger;
  }

  private Trigger createPeriodAndEndCheckerTrigger(final long currMillis) {
    final Map<String, ConditionChecker> triggerCheckers = new HashMap<>();
    final Map<String, ConditionChecker> expireCheckers = new HashMap<>();

    // TODO kunkun-tang: 1 second is the minimum unit for {@link org.joda.time.ReadablePeriod}.
    // In future, we should use some smaller alternative.
    final ConditionChecker triggerChecker = new BasicTimeChecker("BasicTimeChecker_1",
        currMillis, DateTimeZone.UTC, true, true,
        TimeUtils.parsePeriodString("1s"), null);

    // End time is 3 seconds past now.
    final ConditionChecker endTimeChecker = new BasicTimeChecker("EndTimeChecker_1", 111L,
        DateTimeZone.UTC, currMillis + 3000L, false, false,
        null, null);
    triggerCheckers.put(triggerChecker.getId(), triggerChecker);
    expireCheckers.put(endTimeChecker.getId(), endTimeChecker);

    final String triggerExpr = triggerChecker.getId() + ".eval()";
    final String expireExpr = endTimeChecker.getId() + ".eval()";

    final Condition triggerCond = new Condition(triggerCheckers, triggerExpr);
    final Condition expireCond = new Condition(expireCheckers, expireExpr);

    final Trigger timeTrigger = new Trigger.TriggerBuilder("azkaban",
        "",
        triggerCond,
        expireCond,
        getTriggerActions()).build();

    timeTrigger.setResetOnTrigger(false);
    timeTrigger.setResetOnExpire(true);
    return timeTrigger;
  }

  private List<TriggerAction> getTriggerActions() {
    final List<TriggerAction> actions = new ArrayList<>();
    final ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setSlaOptions(Collections.emptyList());
    final TriggerAction act = new ExecuteFlowAction("fuu", 1, "test-project", "test-flow",
        "test-user", executionOptions);
    actions.add(act);
    return actions;
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
}

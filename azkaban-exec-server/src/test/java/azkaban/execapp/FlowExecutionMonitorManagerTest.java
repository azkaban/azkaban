package azkaban.execapp;

import azkaban.sla.SlaOption;
import azkaban.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


public class FlowExecutionMonitorManagerTest {


  private SlaOption createKillingFlowSlaOption(int mins, String type) {
    List<String> slaActions = new ArrayList<String>();
    Map<String, Object> slaInfo = new HashMap<String, Object>();

    slaActions.add(SlaOption.ACTION_CANCEL_FLOW);
    ReadablePeriod dur = Minutes.minutes(mins).toPeriod();
    slaInfo.put(SlaOption.INFO_DURATION, Utils.createPeriodString(dur));
    return new SlaOption(type, slaActions, slaInfo);
  }

  private FlowExecutionMonitorManager createMonitorManager() {
    FlowExecutionMonitorManager monitorManager = new FlowExecutionMonitorManager();
    List<SlaOption> slaOptions = new ArrayList<>();
    slaOptions.add(createKillingFlowSlaOption(1, SlaOption.TYPE_FLOW_FINISH));
    slaOptions.add(createKillingFlowSlaOption(2, SlaOption.TYPE_FLOW_SUCCEED));
    monitorManager.addMonitor(1, slaOptions);

    slaOptions = new ArrayList<>();
    slaOptions.add(createKillingFlowSlaOption(1, SlaOption.TYPE_FLOW_FINISH));
    monitorManager.addMonitor(2, slaOptions);
    return monitorManager;
  }

  @Test
  public void addMonitorTest() {
    FlowExecutionMonitorManager monitorManager = createMonitorManager();
    Trigger[] triggers = monitorManager.getAllTriggers();
    Assert.assertEquals(3, triggers.length);

    Set<String> expectedTriggerStr = new HashSet<>();
    expectedTriggerStr.add("Trigger for execution 1 with trigger condition of slaFailChecker.isSlaFailed() and expire condition of slaPassChecker.isSlaPassed(), KillExecutionAction for 1");
    expectedTriggerStr.add("Trigger for execution 1 with trigger condition of slaFailChecker.isSlaFailed() and expire condition of slaPassChecker.isSlaPassed(), KillExecutionAction for 1");
    expectedTriggerStr.add("Trigger for execution 2 with trigger condition of slaFailChecker.isSlaFailed() and expire condition of slaPassChecker.isSlaPassed(), KillExecutionAction for 2");

    for(Trigger trigger : triggers) {
      Assert.assertTrue(expectedTriggerStr.contains(trigger.toString()));
    }
  }
}

package azkaban.ramppolicy;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableRamp;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class SimpleQuickRampPolicyTest {
  private static final String PROJECT_ID = "spark-start-kit2";
  private static final String PROJECT_ID2 = "spark-start-kit-2";
  private static final String FLOW_ID = "countByCountryFlow";
  private static final String RAMP_ID = "dali-spark";
  private static final String RAMP_POLICY = "SimpleRampPolicy";
  private long timeStamp = 0L;

  @Before
  public void setup() throws Exception {
    timeStamp = System.currentTimeMillis();
  }

  @Test
  public void Test_DisabledRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 100, false
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_PausedRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        true, 100, true
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_ZeroPercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 0, true
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_1StageRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 1, true
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_2StageRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 2, true
    );
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_2StageRamp2() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID2);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 2, true
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_3StageRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 3, true
    );
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_3StageRamp2() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID2);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 3, true
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_FullRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 4, true
    );
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_OverflowStageRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 5, true
    );
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_NegativeStageRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleQuickRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, -1, true
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }
}

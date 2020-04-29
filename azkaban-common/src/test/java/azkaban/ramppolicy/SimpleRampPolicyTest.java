package azkaban.ramppolicy;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableRamp;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class SimpleRampPolicyTest {
  private static final String PROJECT_ID = "spark-start-kit2";
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
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

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
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

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
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

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
  public void Test_FullRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 100, true
    );
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_NegativePercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, -20, true
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_OverflowStageRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 101, true
    );
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_1PercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

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
  public void Test_19PercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 19, true
    );
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_20PercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 20, true
    );
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_21PercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, 0, timeStamp,
        6, 2, 0, 4,
        false, 21, true
    );
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }
}

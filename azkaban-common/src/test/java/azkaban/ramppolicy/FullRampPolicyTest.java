package azkaban.ramppolicy;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableRamp;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class FullRampPolicyTest {
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
    RampPolicy policy = new FullRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(4)
            .setMaxFailureToRampDown(3)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(0)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(false)
            .setRampStage(100)
            .setActive(false)
            .build())
        .build();
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_PausedRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new FullRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(4)
            .setMaxFailureToRampDown(3)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(0)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(true)
            .setRampStage(100)
            .setActive(true)
            .build())
        .build();
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_ZeroPercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new FullRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(4)
            .setMaxFailureToRampDown(3)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(0)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(false)
            .setRampStage(0)
            .setActive(true)
            .build())
        .build();
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_NegativePercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new FullRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(4)
            .setMaxFailureToRampDown(3)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(0)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(false)
            .setRampStage(-20)
            .setActive(true)
            .build())
        .build();
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_1PercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new FullRampPolicy(sysProps, privateProps);

    Project project = new Project(1, PROJECT_ID);
    Flow flow = new Flow(FLOW_ID);
    ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
    ExecutableRamp executableRamp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(4)
            .setMaxFailureToRampDown(3)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(0)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(false)
            .setRampStage(1)
            .setActive(true)
            .build())
        .build();
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }
}

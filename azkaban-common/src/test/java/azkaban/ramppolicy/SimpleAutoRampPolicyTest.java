package azkaban.ramppolicy;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableRamp;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.Props;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple Auto Ramp Policy will be divided to 5 stages
 *  stage 1: 5%
 *  stage 2: 20%
 *  stage 3: 50%
 *  stage 4: 75%
 *  stage 5: 100%
 */
public class SimpleAutoRampPolicyTest {
  private static final String PROJECT_ID = "spark-start-kit";
  private static final String FLOW_ID = "countByCountryFlow";
  private static final Long ESCAPED_TIME = 86400000L * 4;
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
    RampPolicy policy = new SimpleAutoRampPolicy(sysProps, privateProps);

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
    RampPolicy policy = new SimpleAutoRampPolicy(sysProps, privateProps);

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
            .setRampStage(5)
            .setActive(true)
            .build())
        .build();
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_ZeroPercentRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleAutoRampPolicy(sysProps, privateProps);

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
  public void Test_FullRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleAutoRampPolicy(sysProps, privateProps);

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
            .setRampStage(4)
            .setActive(true)
            .build())
        .build();
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_OverflowStageRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleAutoRampPolicy(sysProps, privateProps);

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
            .setRampStage(6)
            .setActive(true)
            .build())
        .build();
    Assert.assertTrue(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_NegativeStageRamp() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleAutoRampPolicy(sysProps, privateProps);

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
            .setRampStage(-1)
            .setActive(true)
            .build())
        .build();
    Assert.assertFalse(policy.check(executableFlow, executableRamp));
  }

  @Test
  public void Test_StageRampWithoutRampModification() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleAutoRampPolicy(sysProps, privateProps);

    Map<Integer, String> projects = new HashMap();
    projects.put(1, "spark-startkit"); // in 5%
    projects.put(2, "spark-start-kit2");  // in 20%
    projects.put(3, "spark-start-kit3");  // in 45%
    projects.put(4, "spark-start-kit4");  // in 70%
    projects.put(5, "spark-start-kit5");  // in 95%

    for (int stage = 1; stage <= 5; stage++) {
      for(Map.Entry<Integer, String> item : projects.entrySet()) {
        Project project = new Project(1, item.getValue());
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
                .setRampStage(stage)
                .setActive(true)
                .build())
            .build();
        Assert.assertEquals(item.getKey() <= stage, policy.check(executableFlow, executableRamp));
      }
    }
  }

  @Test
  public void Test_StageRampWithRampModification() throws Exception {
    Props sysProps = new Props();
    Props privateProps = new Props();
    RampPolicy policy = new SimpleAutoRampPolicy(sysProps, privateProps);

    Map<Integer, String> projects = new HashMap();
    projects.put(1, "spark-startkit"); // in 5%
    projects.put(2, "spark-start-kit2");  // in 20%
    projects.put(3, "spark-start-kit3");  // in 45%
    projects.put(4, "spark-start-kit4");  // in 70%
    projects.put(5, "spark-start-kit5");  // in 95%

    for (int stage = 1; stage <= 5; stage++) {
      for(Map.Entry<Integer, String> item : projects.entrySet()) {
        Project project = new Project(1, item.getValue());
        Flow flow = new Flow(FLOW_ID);
        ExecutableFlow executableFlow = new ExecutableFlow(project, flow);
        ExecutableRamp executableRamp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(4)
                .setMaxFailureToRampDown(3)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(timeStamp - ESCAPED_TIME)
                .setEndTime(0)
                .setLastUpdatedTime(timeStamp)
                .setNumOfTrail(6)
                .setNumOfSuccess(2)
                .setNumOfFailure(0)
                .setNumOfIgnored(4)
                .setPaused(false)
                .setRampStage(stage)
                .setActive(true)
                .build())
            .build();
        Assert.assertEquals(item.getKey() <= stage + 1, policy.check(executableFlow, executableRamp));
      }
    }
  }
}

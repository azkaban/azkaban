package azkaban.executor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ExecutableRampMapTest {
  private static final String RAMP1 = "daliSpark";
  private static final String RAMP2 = "daliPig";
  private static final String RAMP_POLICY1 = "SimpleQuickRampPolicy";
  private static final String RAMP_POLICY2 = "SimpleRampPolicy";

  private ExecutableRampMap executableMap;
  private long timeStamp = 0L;

  @Before
  public void setup() throws Exception {
    executableMap = ExecutableRampMap.createInstance();
    timeStamp = System.currentTimeMillis();
  }

  @Test
  public void testNoActivatedRamp() throws Exception {
    Assert.assertTrue(executableMap.getActivatedAll().isEmpty());
    executableMap.add(RAMP1,
        ExecutableRamp.builder(RAMP1, RAMP_POLICY1)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(500)
                .setMaxFailureToRampDown(500)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(0)
                .setEndTime(0)
                .setLastUpdatedTime(0)
                .setNumOfTrail(0)
                .setNumOfSuccess(0)
                .setNumOfFailure(0)
                .setNumOfIgnored(0)
                .setPaused(false)
                .setRampStage(1)
                .setActive(true)
                .build())
            .build()
    );
    Assert.assertFalse(executableMap.getActivatedAll().isEmpty());

    executableMap.delete(RAMP1);
    executableMap.add(RAMP1,

        ExecutableRamp.builder(RAMP1, RAMP_POLICY1)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(500)
                .setMaxFailureToRampDown(500)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(0)
                .setEndTime(0)
                .setLastUpdatedTime(0)
                .setNumOfTrail(0)
                .setNumOfSuccess(0)
                .setNumOfFailure(0)
                .setNumOfIgnored(0)
                .setPaused(false)
                .setRampStage(1)
                .setActive(false)
                .build())
            .build()
    );
    Assert.assertTrue(executableMap.getActivatedAll().isEmpty());
  }

  @Test
  public void testEmptySet() {
    Assert.assertTrue(executableMap.isEmpty());
    Assert.assertTrue(executableMap.getAll().isEmpty());

    executableMap.add(RAMP1,
        ExecutableRamp.builder(RAMP1, RAMP_POLICY1)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(500)
                .setMaxFailureToRampDown(500)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(0)
                .setEndTime(0)
                .setLastUpdatedTime(0)
                .setNumOfTrail(0)
                .setNumOfSuccess(0)
                .setNumOfFailure(0)
                .setNumOfIgnored(0)
                .setPaused(false)
                .setRampStage(1)
                .setActive(true)
                .build())
            .build()
    );
    Assert.assertFalse(executableMap.getAll().isEmpty());
  }

  @Test
  public void testAddItem() {
    executableMap.add(RAMP1,
        ExecutableRamp.builder(RAMP1, RAMP_POLICY1)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(500)
                .setMaxFailureToRampDown(500)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(0)
                .setEndTime(0)
                .setLastUpdatedTime(0)
                .setNumOfTrail(0)
                .setNumOfSuccess(0)
                .setNumOfFailure(0)
                .setNumOfIgnored(0)
                .setPaused(false)
                .setRampStage(1)
                .setActive(true)
                .build())
            .build()
    );
    Assert.assertEquals(1, executableMap.elementCount());

    executableMap.add(RAMP2,
        ExecutableRamp.builder(RAMP2, RAMP_POLICY2)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(500)
                .setMaxFailureToRampDown(500)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(0)
                .setEndTime(0)
                .setLastUpdatedTime(0)
                .setNumOfTrail(0)
                .setNumOfSuccess(0)
                .setNumOfFailure(0)
                .setNumOfIgnored(0)
                .setPaused(false)
                .setRampStage(1)
                .setActive(true)
                .build())
            .build()
    );
    Assert.assertEquals(2, executableMap.elementCount());
  }

  @Test
  public void testRefreshObject() {
    executableMap.add(RAMP1,
        ExecutableRamp.builder(RAMP1, RAMP_POLICY1)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(500)
                .setMaxFailureToRampDown(500)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(0)
                .setEndTime(0)
                .setLastUpdatedTime(0)
                .setNumOfTrail(0)
                .setNumOfSuccess(0)
                .setNumOfFailure(0)
                .setNumOfIgnored(0)
                .setPaused(false)
                .setRampStage(1)
                .setActive(true)
                .build())
            .build()
    );
    executableMap.add(RAMP2,
        ExecutableRamp.builder(RAMP2, RAMP_POLICY2)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(500)
                .setMaxFailureToRampDown(500)
                .setPercentageScaleForMaxFailure(false)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(0)
                .setEndTime(0)
                .setLastUpdatedTime(0)
                .setNumOfTrail(0)
                .setNumOfSuccess(0)
                .setNumOfFailure(0)
                .setNumOfIgnored(0)
                .setPaused(false)
                .setRampStage(1)
                .setActive(true)
                .build())
            .build()
    );
    Assert.assertEquals(2, executableMap.elementCount());

    ExecutableRampMap novaExecutableMap = ExecutableRampMap.createInstance();
    novaExecutableMap.add(RAMP1,
        ExecutableRamp.builder(RAMP1, RAMP_POLICY2)
            .setMetadata(ExecutableRamp.Metadata.builder()
                .setMaxFailureToPause(5000)
                .setMaxFailureToRampDown(50)
                .setPercentageScaleForMaxFailure(true)
                .build())
            .setState(ExecutableRamp.State.builder()
                .setStartTime(timeStamp)
                .setEndTime(timeStamp + 10)
                .setLastUpdatedTime(timeStamp + 5)
                .setNumOfTrail(1)
                .setNumOfSuccess(2)
                .setNumOfFailure(3)
                .setNumOfIgnored(4)
                .setPaused(true)
                .setRampStage(2)
                .setActive(false)
                .build())
            .build()
    );
    executableMap.refresh(novaExecutableMap);
    Assert.assertEquals(1, executableMap.elementCount());
    Assert.assertEquals(RAMP_POLICY2, executableMap.get(RAMP1).getPolicy());
    Assert.assertEquals(5000, executableMap.get(RAMP1).getMaxFailureToPause());
    Assert.assertEquals(50, executableMap.get(RAMP1).getMaxFailureToRampDown());
    Assert.assertTrue(executableMap.get(RAMP1).isPercentageScaleForMaxFailure());
    Assert.assertEquals(2, executableMap.get(RAMP1).getStage());
    Assert.assertFalse(executableMap.get(RAMP1).isActive());
    Assert.assertNull(executableMap.get(RAMP2));
    Assert.assertTrue(executableMap.get(RAMP1).isPaused());
    Assert.assertEquals(4, executableMap.get(RAMP1).getCount(ExecutableRamp.CountType.IGNORED));
    Assert.assertEquals(3, executableMap.get(RAMP1).getCount(ExecutableRamp.CountType.FAILURE));
    Assert.assertEquals(2, executableMap.get(RAMP1).getCount(ExecutableRamp.CountType.SUCCESS));
    Assert.assertEquals(1, executableMap.get(RAMP1).getCount(ExecutableRamp.CountType.TRAIL));
    Assert.assertEquals(timeStamp + 5, executableMap.get(RAMP1).getLastUpdatedTime());
    Assert.assertEquals(timeStamp + 10, executableMap.get(RAMP1).getEndTime());
    Assert.assertEquals(timeStamp, executableMap.get(RAMP1).getStartTime());
  }
}

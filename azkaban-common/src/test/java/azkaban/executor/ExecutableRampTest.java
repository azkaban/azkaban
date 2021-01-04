package azkaban.executor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ExecutableRampTest {
  private static final String RAMP_ID = "dali-spark";
  private static final String RAMP_POLICY = "SimpleRampPolicy";

  private ExecutableRamp executableRamp;
  private long timeStamp = 0L;

  @Before
  public void setup() throws Exception {
    timeStamp = System.currentTimeMillis();
    executableRamp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(4)
            .setMaxFailureToRampDown(3)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(timeStamp + 10)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(false)
            .setRampStage(2)
            .setActive(true)
            .build())
        .build();
  }

  @Test
  public void testGet() {
    Assert.assertTrue(executableRamp.isActive());
    Assert.assertFalse(executableRamp.ignoreTestFailure());
    Assert.assertEquals(RAMP_POLICY, executableRamp.getPolicy());
    Assert.assertEquals(4, executableRamp.getMaxFailureToPause());
    Assert.assertEquals(3, executableRamp.getMaxFailureToRampDown());
    Assert.assertFalse(executableRamp.isPercentageScaleForMaxFailure());
    Assert.assertEquals(2, executableRamp.getStage());
    Assert.assertTrue(executableRamp.isActive());
    Assert.assertFalse(executableRamp.isPaused());
    Assert.assertEquals(4, executableRamp.getCount(ExecutableRamp.CountType.IGNORED));
    Assert.assertEquals(0, executableRamp.getCount(ExecutableRamp.CountType.FAILURE));
    Assert.assertEquals(2, executableRamp.getCount(ExecutableRamp.CountType.SUCCESS));
    Assert.assertEquals(6, executableRamp.getCount(ExecutableRamp.CountType.TRAIL));
    Assert.assertEquals(timeStamp, executableRamp.getLastUpdatedTime());
    Assert.assertEquals(timeStamp + 10, executableRamp.getEndTime());
    Assert.assertEquals(timeStamp, executableRamp.getStartTime());
  }

  @Test
  public void testCache() {
    executableRamp.cacheResult(ExecutableRamp.Action.FAILED);
    executableRamp.cacheResult(ExecutableRamp.Action.FAILED);
    executableRamp.cacheResult(ExecutableRamp.Action.SUCCEEDED);
    executableRamp.cacheResult(ExecutableRamp.Action.SUCCEEDED);
    executableRamp.cacheResult(ExecutableRamp.Action.SUCCEEDED);
    executableRamp.cacheResult(ExecutableRamp.Action.SUCCEEDED);
    executableRamp.cacheResult(ExecutableRamp.Action.IGNORED);

    Assert.assertTrue(executableRamp.isActive());
    Assert.assertEquals(RAMP_POLICY, executableRamp.getPolicy());
    Assert.assertEquals(4, executableRamp.getMaxFailureToPause());
    Assert.assertEquals(3, executableRamp.getMaxFailureToRampDown());
    Assert.assertFalse(executableRamp.isPercentageScaleForMaxFailure());
    Assert.assertEquals(2, executableRamp.getStage());
    Assert.assertTrue(executableRamp.isActive());
    Assert.assertFalse(executableRamp.isPaused());
    Assert.assertEquals(4, executableRamp.getCount(ExecutableRamp.CountType.IGNORED));
    Assert.assertEquals(0, executableRamp.getCount(ExecutableRamp.CountType.FAILURE));
    Assert.assertEquals(2, executableRamp.getCount(ExecutableRamp.CountType.SUCCESS));
    Assert.assertEquals(6, executableRamp.getCount(ExecutableRamp.CountType.TRAIL));
    Assert.assertTrue(executableRamp.getLastUpdatedTime() - timeStamp >= 0);
    Assert.assertEquals(timeStamp + 10, executableRamp.getEndTime());
    Assert.assertEquals(timeStamp, executableRamp.getStartTime());
    Assert.assertTrue(executableRamp.isChanged());

    executableRamp.cacheSaved();
    Assert.assertTrue(executableRamp.isActive());
    Assert.assertEquals(RAMP_POLICY, executableRamp.getPolicy());
    Assert.assertEquals(4, executableRamp.getMaxFailureToPause());
    Assert.assertEquals(3, executableRamp.getMaxFailureToRampDown());
    Assert.assertFalse(executableRamp.isPercentageScaleForMaxFailure());
    Assert.assertEquals(2, executableRamp.getStage());
    Assert.assertTrue(executableRamp.isActive());
    Assert.assertFalse(executableRamp.isPaused());
    Assert.assertEquals(5, executableRamp.getCount(ExecutableRamp.CountType.IGNORED));
    Assert.assertEquals(2, executableRamp.getCount(ExecutableRamp.CountType.FAILURE));
    Assert.assertEquals(6, executableRamp.getCount(ExecutableRamp.CountType.SUCCESS));
    Assert.assertEquals(13, executableRamp.getCount(ExecutableRamp.CountType.TRAIL));
    Assert.assertTrue(executableRamp.getLastUpdatedTime() - timeStamp >= 0);
    Assert.assertEquals(timeStamp + 10, executableRamp.getEndTime());
    Assert.assertEquals(timeStamp, executableRamp.getStartTime());
    Assert.assertFalse(executableRamp.isChanged());

    executableRamp.cacheResult(ExecutableRamp.Action.FAILED);
    executableRamp.cacheResult(ExecutableRamp.Action.FAILED);
    Assert.assertTrue(executableRamp.isActive());
    Assert.assertEquals(1, executableRamp.getStage());

    executableRamp.cacheResult(ExecutableRamp.Action.FAILED);
    Assert.assertFalse(executableRamp.isActive());
    Assert.assertEquals(1, executableRamp.getStage());
  }

  @Test
  public void testClone() {
    ExecutableRamp cloned = executableRamp.clone();
    Assert.assertTrue(cloned.isActive());
    Assert.assertFalse(cloned.ignoreTestFailure());
    Assert.assertEquals(RAMP_POLICY, cloned.getPolicy());
    Assert.assertEquals(4, cloned.getMaxFailureToPause());
    Assert.assertEquals(3, cloned.getMaxFailureToRampDown());
    Assert.assertFalse(cloned.isPercentageScaleForMaxFailure());
    Assert.assertEquals(2, cloned.getStage());
    Assert.assertTrue(cloned.isActive());
    Assert.assertFalse(cloned.isPaused());
    Assert.assertEquals(4, cloned.getCount(ExecutableRamp.CountType.IGNORED));
    Assert.assertEquals(0, cloned.getCount(ExecutableRamp.CountType.FAILURE));
    Assert.assertEquals(2, cloned.getCount(ExecutableRamp.CountType.SUCCESS));
    Assert.assertEquals(6, cloned.getCount(ExecutableRamp.CountType.TRAIL));
    Assert.assertEquals(timeStamp, cloned.getLastUpdatedTime());
    Assert.assertEquals(timeStamp + 10, executableRamp.getEndTime());
    Assert.assertEquals(timeStamp, cloned.getStartTime());
  }

  @Test
  public void testMagneticTestFlag() {
    ExecutableRamp ramp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(-1)
            .setMaxFailureToRampDown(3)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(timeStamp + 10)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(false)
            .setRampStage(2)
            .setActive(true)
            .build())
        .build();
    Assert.assertFalse(ramp.ignoreTestFailure());

    ramp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(3)
            .setMaxFailureToRampDown(-1)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(timeStamp + 10)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(false)
            .setRampStage(2)
            .setActive(true)
            .build())
        .build();
    Assert.assertFalse(ramp.ignoreTestFailure());

    ramp = ExecutableRamp.builder(RAMP_ID, RAMP_POLICY)
        .setMetadata(ExecutableRamp.Metadata.builder()
            .setMaxFailureToPause(-1)
            .setMaxFailureToRampDown(-1)
            .setPercentageScaleForMaxFailure(false)
            .build())
        .setState(ExecutableRamp.State.builder()
            .setStartTime(timeStamp)
            .setEndTime(timeStamp + 10)
            .setLastUpdatedTime(timeStamp)
            .setNumOfTrail(6)
            .setNumOfSuccess(2)
            .setNumOfFailure(0)
            .setNumOfIgnored(4)
            .setPaused(false)
            .setRampStage(2)
            .setActive(true)
            .build())
        .build();
    Assert.assertTrue(ramp.ignoreTestFailure());
  }
}

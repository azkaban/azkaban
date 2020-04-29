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
    executableRamp = ExecutableRamp.createInstance(
        RAMP_ID,
        RAMP_POLICY,
        4, 3, false,
        timeStamp, timeStamp + 10, timeStamp,
        6, 2, 0, 4,
        false, 2, true
    );
  }

  @Test
  public void testGet() {
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
}

package azkaban.trigger;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import azkaban.trigger.builtin.NyxTriggerChecker;
import azkaban.utils.NyxUtils;

/**
 * Test class for NyxTriggerChecker
 *
 * @author gaggarwa
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ NyxUtils.class, NyxTriggerChecker.class })
public class NyxTriggerCheckerTest {
  private NyxTriggerChecker checker;
  private String dummySpec = "spec1";
  private long dummyTriggerId = 123L;

  @Before
  public void initObjects() throws Exception {
    PowerMockito.mockStatic(NyxUtils.class);
    PowerMockito.when(NyxUtils.registerNyxTrigger(dummySpec)).thenReturn(
        dummyTriggerId);
    PowerMockito.doNothing().when(NyxUtils.class, "unregisterNyxTrigger",
        dummyTriggerId);
    PowerMockito.when(NyxUtils.isNyxTriggerReady(dummyTriggerId)).thenReturn(
        true);

    checker = new NyxTriggerChecker(dummySpec, "NyxTriggerChecker_1");
  }

  @Test
  public void testDefaultTriggerRegister() throws TriggerManagerException {
    Assert.assertEquals("NyxTriggerChecker_1", checker.getId());
    Assert.assertEquals(-1, checker.getTriggerId());
  }

  @Test
  public void testJSONserialization() throws Exception {
    @SuppressWarnings("unchecked")
    ConditionChecker retrievedChecker =
        NyxTriggerChecker.createFromJson((HashMap<String, Object>) checker
            .toJson());
    Assert.assertEquals(checker, retrievedChecker);
  }

  @Test
  public void testReset() throws TriggerManagerException {
    checker.eval();
    checker.reset();
    PowerMockito.verifyStatic();
    NyxUtils.unregisterNyxTrigger(dummyTriggerId);

    PowerMockito.verifyStatic(Mockito.times(2));
    NyxUtils.registerNyxTrigger(dummySpec);
  }

  @Test
  public void testStop() throws TriggerManagerException {
    checker.eval();
    checker.stopChecker();
    PowerMockito.verifyStatic();
    NyxUtils.unregisterNyxTrigger(dummyTriggerId);
  }

  @Test
  public void testType() {
    Assert.assertEquals(NyxTriggerChecker.type, checker.getType());
  }

  @Test
  public void testGetNum() {
    Assert.assertEquals(null, checker.getNum());
  }

  @Test
  public void testGetNextCheckTime() {
    Assert.assertEquals(Long.MAX_VALUE, checker.getNextCheckTime());
  }

  @Test
  public void testEval() {
    Assert.assertEquals(true, checker.eval());
  }
}

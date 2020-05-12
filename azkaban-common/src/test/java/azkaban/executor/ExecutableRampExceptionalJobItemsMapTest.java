package azkaban.executor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ExecutableRampExceptionalJobItemsMapTest {
  private static final String RAMP_ID1 = "daliSpark";
  private static final String RAMP_ID2 = "daliPig";
  private static final String FLOW_NAME1 = "project1.flow1";
  private static final String FLOW_NAME2 = "project1.flow2";
  private static final String JOB_NAME1 = "job1";
  private static final String JOB_NAME2 = "job2";

  private ExecutableRampExceptionalJobItemsMap executableMap;
  private long timeStamp = 0L;

  @Before
  public void setup() throws Exception {
    executableMap = ExecutableRampExceptionalJobItemsMap.createInstance();
    timeStamp = System.currentTimeMillis();
  }

  @Test
  public void testEmptySet() {
    Assert.assertTrue(executableMap.isEmpty());
  }

  @Test
  public void testAddItem() {
    executableMap.add(RAMP_ID1, FLOW_NAME1, JOB_NAME1, ExecutableRampStatus.BLACKLISTED, timeStamp);
    Assert.assertEquals(1, executableMap.size());
    Assert.assertEquals(ExecutableRampStatus.BLACKLISTED, executableMap.check(RAMP_ID1, FLOW_NAME1, JOB_NAME1));
    Assert.assertEquals(ExecutableRampStatus.UNDETERMINED, executableMap.check(RAMP_ID1, FLOW_NAME2, JOB_NAME2));
    Assert.assertEquals(ExecutableRampStatus.UNDETERMINED, executableMap.check(RAMP_ID2, FLOW_NAME1, JOB_NAME1));
    Assert.assertEquals(1, executableMap.elementCount());

    executableMap.add(RAMP_ID1, FLOW_NAME1, JOB_NAME2, ExecutableRampStatus.WHITELISTED, timeStamp);
    Assert.assertEquals(1, executableMap.size());
    Assert.assertEquals(2, executableMap.get(RAMP_ID1, FLOW_NAME1).getItems().size());
    Assert.assertEquals(ExecutableRampStatus.BLACKLISTED, executableMap.check(RAMP_ID1, FLOW_NAME1, JOB_NAME1));
    Assert.assertEquals(ExecutableRampStatus.WHITELISTED, executableMap.check(RAMP_ID1, FLOW_NAME1, JOB_NAME2));
    Assert.assertEquals(2, executableMap.elementCount());

    executableMap.add(RAMP_ID1, FLOW_NAME2, JOB_NAME2, ExecutableRampStatus.SELECTED, timeStamp);
    Assert.assertEquals(2, executableMap.size());
    Assert.assertEquals(ExecutableRampStatus.SELECTED, executableMap.check(RAMP_ID1, FLOW_NAME2, JOB_NAME2));
    Assert.assertEquals(3, executableMap.elementCount());

    executableMap.add(RAMP_ID1, FLOW_NAME2, JOB_NAME2, ExecutableRampStatus.UNSELECTED, timeStamp);
    Assert.assertEquals(2, executableMap.size());
    Assert.assertEquals(ExecutableRampStatus.UNSELECTED, executableMap.check(RAMP_ID1, FLOW_NAME2, JOB_NAME2));
    Assert.assertEquals(3, executableMap.elementCount());
  }


  @Test
  public void testRefreshObject() {
    executableMap.add(RAMP_ID1, FLOW_NAME1, JOB_NAME1, ExecutableRampStatus.BLACKLISTED, timeStamp);
    executableMap.add(RAMP_ID1, FLOW_NAME1, JOB_NAME2, ExecutableRampStatus.WHITELISTED, timeStamp);
    executableMap.add(RAMP_ID2, FLOW_NAME2, JOB_NAME2, ExecutableRampStatus.BLACKLISTED, timeStamp);

    Assert.assertEquals(ExecutableRampStatus.BLACKLISTED, executableMap.check(RAMP_ID1, FLOW_NAME1, JOB_NAME1));
    Assert.assertEquals(ExecutableRampStatus.WHITELISTED, executableMap.check(RAMP_ID1, FLOW_NAME1, JOB_NAME2));
    Assert.assertEquals(ExecutableRampStatus.BLACKLISTED, executableMap.check(RAMP_ID2, FLOW_NAME2, JOB_NAME2));
    Assert.assertEquals(timeStamp, executableMap.get(RAMP_ID1, FLOW_NAME1).getItems().get(JOB_NAME1).getTimeStamp());

    Assert.assertTrue(executableMap.exists(RAMP_ID1, FLOW_NAME1, JOB_NAME1));
    Assert.assertTrue(executableMap.exists(RAMP_ID1, FLOW_NAME1, JOB_NAME2));
    Assert.assertFalse(executableMap.exists(RAMP_ID2, FLOW_NAME2, JOB_NAME1));
    Assert.assertTrue(executableMap.exists(RAMP_ID2, FLOW_NAME2, JOB_NAME2));

    ExecutableRampExceptionalJobItemsMap novaExecutableMap = ExecutableRampExceptionalJobItemsMap.createInstance();
    novaExecutableMap.add(RAMP_ID1, FLOW_NAME1, JOB_NAME1, ExecutableRampStatus.WHITELISTED, timeStamp + 1);
    novaExecutableMap.add(RAMP_ID1, FLOW_NAME2, JOB_NAME1, ExecutableRampStatus.BLACKLISTED, timeStamp + 1);
    novaExecutableMap.add(RAMP_ID2, FLOW_NAME2, JOB_NAME2, ExecutableRampStatus.BLACKLISTED, timeStamp + 1);

    executableMap.refresh(novaExecutableMap);
    Assert.assertEquals(ExecutableRampStatus.WHITELISTED, executableMap.check(RAMP_ID1, FLOW_NAME1, JOB_NAME1));
    Assert.assertEquals(ExecutableRampStatus.UNDETERMINED, executableMap.check(RAMP_ID1, FLOW_NAME1, JOB_NAME2));
    Assert.assertEquals(ExecutableRampStatus.BLACKLISTED, executableMap.check(RAMP_ID1, FLOW_NAME2, JOB_NAME1));
    Assert.assertEquals(ExecutableRampStatus.BLACKLISTED, executableMap.check(RAMP_ID2, FLOW_NAME2, JOB_NAME2));
    Assert.assertEquals(timeStamp + 1, executableMap.get(RAMP_ID1, FLOW_NAME1).getItems().get(JOB_NAME1).getTimeStamp());

    Assert.assertTrue(executableMap.exists(RAMP_ID1, FLOW_NAME1, JOB_NAME1));
    Assert.assertFalse(executableMap.exists(RAMP_ID1, FLOW_NAME1, JOB_NAME2));
    Assert.assertTrue(executableMap.exists(RAMP_ID1, FLOW_NAME2, JOB_NAME1));
    Assert.assertTrue(executableMap.exists(RAMP_ID2, FLOW_NAME2, JOB_NAME2));
  }
}

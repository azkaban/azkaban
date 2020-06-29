package azkaban.executor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ExecutableRampItemsMapTest {
  private static final String RAMP = "dali";
  private static final String RAMP1 = "daliSpark";
  private static final String RAMP2 = "daliPig";
  private static final String DEPENDENCY1 = "cfg:spark.cluster.jars";
  private static final String DEPENDENCY2 = "reg:pig";
  private static final String RAMPVALUE0 = "dali-data-spark:/../dali-data-spark-9-all.jar";
  private static final String RAMPVALUE1 = "dali-data-spark:/../dali-data-spark-9-104.jar";
  private static final String RAMPVALUE2 = "dali-data-pig:/../dali-data-pig-9-104.jar";

  private ExecutableRampItemsMap executableMap;
  private long timeStamp = 0L;

  @Before
  public void setup() throws Exception {
    executableMap = ExecutableRampItemsMap.createInstance();
    timeStamp = System.currentTimeMillis();
  }

  @Test
  public void testEmptySet() {
    Assert.assertTrue(executableMap.isEmpty());
  }

  @Test
  public void testAddItem() {
    executableMap.add(RAMP, DEPENDENCY1, RAMPVALUE1);
    Assert.assertEquals(1, executableMap.elementCount());
    Assert.assertNotNull(executableMap.getRampItems(RAMP));
    Assert.assertEquals(RAMPVALUE1, executableMap.getRampItems(RAMP).get(DEPENDENCY1));
    Assert.assertEquals(1, executableMap.getDependencies(RAMP).size());
    Assert.assertTrue(executableMap.getDependencies(RAMP).contains(DEPENDENCY1));
    Assert.assertFalse(executableMap.getDependencies(RAMP).contains(DEPENDENCY2));
    Assert.assertNotNull(executableMap.getRampItems(RAMP2));
    Assert.assertEquals(0, executableMap.getRampItems(RAMP2).size());
    Assert.assertEquals(0, executableMap.getDependencies(RAMP2).size());

    executableMap.add(RAMP, DEPENDENCY2, RAMPVALUE2);
    Assert.assertEquals(2, executableMap.elementCount());
    Assert.assertEquals(2, executableMap.getDependencies(RAMP).size());
  }

  @Test
  public void testRefreshObject() {
    executableMap.add(RAMP, DEPENDENCY1, RAMPVALUE0);
    executableMap.add(RAMP1, DEPENDENCY1, RAMPVALUE0);
    Assert.assertEquals(2, executableMap.elementCount());
    Assert.assertEquals(1, executableMap.getDependencies(RAMP).size());
    Assert.assertEquals(1, executableMap.getDependencies(RAMP1).size());

    ExecutableRampItemsMap novaExecutableMap = ExecutableRampItemsMap.createInstance();
    novaExecutableMap.add(RAMP, DEPENDENCY1, RAMPVALUE1);
    novaExecutableMap.add(RAMP, DEPENDENCY2, RAMPVALUE2);

    executableMap.refresh(novaExecutableMap);
    Assert.assertEquals(2, executableMap.elementCount());
    Assert.assertEquals(2, executableMap.getDependencies(RAMP).size());
    Assert.assertEquals(0, executableMap.getDependencies(RAMP1).size());
    Assert.assertEquals(RAMPVALUE1, executableMap.getRampItems(RAMP).get(DEPENDENCY1));
  }
}

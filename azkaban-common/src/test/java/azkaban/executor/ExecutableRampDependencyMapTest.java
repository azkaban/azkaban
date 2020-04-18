package azkaban.executor;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ExecutableRampDependencyMapTest {
  private static final String DEPENDENCY_NAME0 = "UNKNOWN";
  private static final String DEPENDENCY_NAME1 = "daliSpark";
  private static final String DEPENDENCY_NAME2 = "daliPig";
  private static final String DEPENDENCY_NAME3 = "dali";
  private static final String DEPENDENCY_NAME5 = "dali2";
  private static final String DEPENDENCY_NAME6 = "dali3";
  private static final String DEPENDENCY_VALUE11 = "dali-data-spark:/../dali-data-spark-9-all.jar";
  private static final String DEPENDENCY_VALUE12 = "dali-data-spark:/../dali-data-spark-9-104.jar";
  private static final String DEPENDENCY_VALUE21 = "dali-data-pig:/../dali-data-pig-9-all.jar";
  private static final String DEPENDENCY_VALUE22 = "dali-data-pig:/../dali-data-pig-9-104.jar";
  private static final String JOB_TYPE_SPARK = "Spark";
  private static final String JOB_TYPE_PIG = "PigLi";
  private static final String JOB_TYPE_MR = "MR";
  private static final String JOB_TYPE_SPARK_PIG = "Spark,PigLi";
  private static final String JOB_TYPE_ALL = "";

  private ExecutableRampDependencyMap executableMap;

  @Before
  public void setup() throws Exception {
    executableMap = ExecutableRampDependencyMap.createInstance();
  }

  @Test
  public void testEmptySet() {
    Assert.assertTrue(executableMap.isEmpty());
  }

  @Test
  public void testAddDependency() {
    executableMap.add(DEPENDENCY_NAME1, DEPENDENCY_VALUE11, JOB_TYPE_SPARK);
    Assert.assertFalse(executableMap.isEmpty());
    Assert.assertEquals(1, executableMap.size());
    Assert.assertEquals(DEPENDENCY_VALUE11, executableMap.getDefaultValue(DEPENDENCY_NAME1));
    executableMap.add(DEPENDENCY_NAME2, DEPENDENCY_VALUE21, JOB_TYPE_PIG);
    Assert.assertEquals(2, executableMap.size());
    Assert.assertNull(executableMap.getDefaultValue(DEPENDENCY_NAME3));
    executableMap.add(DEPENDENCY_NAME2, DEPENDENCY_VALUE22, JOB_TYPE_PIG);
    Assert.assertEquals(2, executableMap.size());
    Assert.assertEquals(DEPENDENCY_VALUE22, executableMap.getDefaultValue(DEPENDENCY_NAME2));
  }

  @Test
  public void testAddDependencies() {
    executableMap.add(DEPENDENCY_NAME1, DEPENDENCY_VALUE11, JOB_TYPE_SPARK);
    executableMap.add(DEPENDENCY_NAME2, DEPENDENCY_VALUE21, JOB_TYPE_PIG);
    Assert.assertEquals(2, executableMap.size());
    Map<String, String> defaultValues = executableMap.getDefaultValues(
        ImmutableSet.<String>builder().add(DEPENDENCY_NAME1).add(DEPENDENCY_NAME2).build()
    );
    Assert.assertEquals(2, defaultValues.size());
    Assert.assertEquals(DEPENDENCY_VALUE11, defaultValues.get(DEPENDENCY_NAME1));
    Assert.assertEquals(DEPENDENCY_VALUE21, defaultValues.get(DEPENDENCY_NAME2));
    defaultValues = executableMap.getDefaultValues(
        Collections.emptySet()
    );
    Assert.assertEquals(0, defaultValues.size());
  }

  @Test
  public void testIsValidJobTypes() {
    executableMap.add(DEPENDENCY_NAME1, DEPENDENCY_VALUE11, JOB_TYPE_SPARK);
    executableMap.add(DEPENDENCY_NAME2, DEPENDENCY_VALUE21, JOB_TYPE_PIG);
    executableMap.add(DEPENDENCY_NAME3, DEPENDENCY_VALUE21, JOB_TYPE_SPARK_PIG);
    executableMap.add(DEPENDENCY_NAME5, DEPENDENCY_VALUE21, JOB_TYPE_ALL);
    executableMap.add(DEPENDENCY_NAME6, DEPENDENCY_VALUE21, null);
    Assert.assertEquals(5, executableMap.size());
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME1, JOB_TYPE_SPARK));
    Assert.assertFalse(executableMap.isValidJobType(DEPENDENCY_NAME1, JOB_TYPE_PIG));
    Assert.assertFalse(executableMap.isValidJobType(DEPENDENCY_NAME2, JOB_TYPE_SPARK));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME2, JOB_TYPE_PIG));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME3, JOB_TYPE_SPARK));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME3, JOB_TYPE_PIG));
    Assert.assertFalse(executableMap.isValidJobType(DEPENDENCY_NAME3, JOB_TYPE_MR));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME5, JOB_TYPE_SPARK));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME5, JOB_TYPE_PIG));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME5, JOB_TYPE_MR));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME6, JOB_TYPE_SPARK));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME6, JOB_TYPE_PIG));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME6, JOB_TYPE_MR));
    Assert.assertFalse(executableMap.isValidJobType(DEPENDENCY_NAME0, JOB_TYPE_SPARK));
  }

  @Test
  public void testUploadDependency() {
    executableMap.add(DEPENDENCY_NAME1, DEPENDENCY_VALUE11, JOB_TYPE_SPARK);
    Assert.assertEquals(1, executableMap.size());
    Assert.assertEquals(DEPENDENCY_VALUE11, executableMap.getDefaultValue(DEPENDENCY_NAME1));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME1, JOB_TYPE_SPARK));
    Assert.assertFalse(executableMap.isValidJobType(DEPENDENCY_NAME1, JOB_TYPE_MR));
    Assert.assertFalse(executableMap.isValidJobType(DEPENDENCY_NAME1, JOB_TYPE_PIG));

    ExecutableRampDependencyMap novaExecutableMap = ExecutableRampDependencyMap.createInstance();
    novaExecutableMap.add(DEPENDENCY_NAME1, DEPENDENCY_VALUE12, JOB_TYPE_SPARK_PIG);
    executableMap.refresh(novaExecutableMap);
    Assert.assertEquals(1, executableMap.size());
    Assert.assertNotEquals(DEPENDENCY_VALUE11, executableMap.getDefaultValue(DEPENDENCY_NAME1));
    Assert.assertEquals(DEPENDENCY_VALUE12, executableMap.getDefaultValue(DEPENDENCY_NAME1));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME1, JOB_TYPE_SPARK));
    Assert.assertFalse(executableMap.isValidJobType(DEPENDENCY_NAME1, JOB_TYPE_MR));
    Assert.assertTrue(executableMap.isValidJobType(DEPENDENCY_NAME1, JOB_TYPE_PIG));
  }

  @Test
  public void testClone() {
    executableMap.add(DEPENDENCY_NAME1, DEPENDENCY_VALUE11, JOB_TYPE_SPARK);
    Assert.assertEquals(1, executableMap.size());
    ExecutableRampDependencyMap clonedExecutableMap = executableMap.clone();
    Assert.assertEquals(clonedExecutableMap, executableMap);
    Assert.assertEquals(DEPENDENCY_VALUE11, executableMap.getDefaultValue(DEPENDENCY_NAME1));
    Assert.assertEquals(DEPENDENCY_VALUE11, clonedExecutableMap.getDefaultValue(DEPENDENCY_NAME1));

    ExecutableRampDependencyMap novaExecutableMap = ExecutableRampDependencyMap.createInstance();
    novaExecutableMap.add(DEPENDENCY_NAME1, DEPENDENCY_VALUE12, JOB_TYPE_SPARK_PIG);
    executableMap.refresh(novaExecutableMap);
    Assert.assertEquals(1, executableMap.size());
    Assert.assertEquals(DEPENDENCY_VALUE12, executableMap.getDefaultValue(DEPENDENCY_NAME1));
    Assert.assertEquals(1, clonedExecutableMap.size());
    Assert.assertEquals(DEPENDENCY_VALUE12, clonedExecutableMap.getDefaultValue(DEPENDENCY_NAME1));
  }
}

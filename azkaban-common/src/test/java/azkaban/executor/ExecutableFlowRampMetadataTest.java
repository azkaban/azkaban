package azkaban.executor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ExecutableFlowRampMetadataTest {

  private static final String FLOW_ID = "test";

  private ExecutableFlowRampMetadata executableMetadata;
  private ExecutableRampExceptionalJobItemsMap executableJobItemsMap;
  private ExecutableRampDependencyMap executableDependencyMap;

  @Before
  public void setup() throws Exception {
    executableDependencyMap = ExecutableRampDependencyMap.createInstance();
    executableJobItemsMap = ExecutableRampExceptionalJobItemsMap.createInstance();
    executableMetadata = ExecutableFlowRampMetadata.createInstance(
        executableDependencyMap,
        executableJobItemsMap.getExceptionalJobItemsByFlow(FLOW_ID)
    );
  }

  @Test
  public void testEmptySet() {
    Assert.assertTrue(executableMetadata.getActiveRamps().isEmpty());
  }
}

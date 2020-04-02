package azkaban.executor;

import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;


public class ExecutableRampMapTest {

  private ExecutableRampMap executableRampMap;

  @Before
  public void setup() throws Exception {
    executableRampMap = new ExecutableRampMap();
  }

  @Test
  public void testNoActivatedRamp() throws Exception {
    assertThat(executableRampMap.getActivatedAll().isEmpty()).isTrue();
  }
}

package azkaban;

import static org.junit.Assert.assertEquals;

import azkaban.Constants.FlowRetryStrategy;
import org.junit.Test;

public class ConstantsTest {

  @Test
  public void testFlowRetryStrategyValueFromName() {
    assertEquals(FlowRetryStrategy.DEFAULT, FlowRetryStrategy.valueFromName("retryAsNew"));
    assertEquals(FlowRetryStrategy.DEFAULT, FlowRetryStrategy.valueFromName("DEFAULT"));
    assertEquals(FlowRetryStrategy.DISABLE_SUCCEEDED_NODES,
        FlowRetryStrategy.valueFromName("disableSucceededNodes"));
    assertEquals(FlowRetryStrategy.DISABLE_SUCCEEDED_NODES,
        FlowRetryStrategy.valueFromName("DISABLE_SUCCEEDED_NODES"));

    for (FlowRetryStrategy strategy: FlowRetryStrategy.values()) {
      assertEquals(strategy, FlowRetryStrategy.valueFromName(strategy.getName()));
      assertEquals(strategy, FlowRetryStrategy.valueFromName(strategy.name()));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFlowRetryStrategyValueFromNameInvalid() {
    assertEquals(FlowRetryStrategy.DEFAULT, FlowRetryStrategy.valueFromName("bad-value"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFlowRetryStrategyValueFromNameEmpty() {
    assertEquals(FlowRetryStrategy.DEFAULT, FlowRetryStrategy.valueFromName(""));
  }
}
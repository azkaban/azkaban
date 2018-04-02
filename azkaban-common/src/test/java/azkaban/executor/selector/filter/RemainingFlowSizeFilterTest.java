package azkaban.executor.selector.filter;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorInfo;
import azkaban.utils.Props;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RemainingFlowSizeFilterTest {

  @Mock
  private Executor executor;

  @Mock
  private ExecutableFlow flow;

  @Mock
  private ExecutorInfo executorInfo;

  @Before
  public void setUp() {
    when(executor.getExecutorInfo()).thenReturn(executorInfo);
  }

  @Test
  public void filterWillUseDefaultIfNotConfigured() {
    RemainingFlowSizeFilter filter = new RemainingFlowSizeFilter("size", new Props());

    when(executorInfo.getRemainingFlowCapacity()).thenReturn(RemainingFlowSizeFilter.DEFAULT_MIN_REMAINING_CAPACITY);
    assertThat(filter.filterTarget(executor, flow)).isFalse();

    when(executorInfo.getRemainingFlowCapacity()).thenReturn(RemainingFlowSizeFilter.DEFAULT_MIN_REMAINING_CAPACITY + 1);
    assertThat(filter.filterTarget(executor, flow)).isTrue();
  }

  @Test
  public void filterShouldUseConfiguredThresholdIfExists() {
    Props props = Props.of(RemainingFlowSizeFilter.MIN_REMAINING_CAPACITY_CONFIG_KEY, "100");
    RemainingFlowSizeFilter filter = new RemainingFlowSizeFilter("size", props);

    when(executorInfo.getRemainingFlowCapacity()).thenReturn(200);
    assertThat(filter.filterTarget(executor, flow)).isTrue();

    when(executorInfo.getRemainingFlowCapacity()).thenReturn(30);
    assertThat(filter.filterTarget(executor, flow)).isFalse();
  }
}

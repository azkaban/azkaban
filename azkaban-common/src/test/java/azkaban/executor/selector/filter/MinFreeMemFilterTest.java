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
public class MinFreeMemFilterTest {

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
    MinFreeMemFilter filter = new MinFreeMemFilter("mem", new Props());

    when(executorInfo.getRemainingMemoryInMB()).thenReturn(MinFreeMemFilter.DEFAULT_MIN_FREE_MEM_IN_MB);
    assertThat(filter.filterTarget(executor, flow)).isFalse();

    when(executorInfo.getRemainingMemoryInMB()).thenReturn(MinFreeMemFilter.DEFAULT_MIN_FREE_MEM_IN_MB + 1L);
    assertThat(filter.filterTarget(executor, flow)).isTrue();
  }

  @Test
  public void filterShouldUseConfiguredThresholdIfExists() {
    Props props = Props.of(MinFreeMemFilter.MIN_FREE_MEM_IN_MB_CONFIG_KEY, "100");
    MinFreeMemFilter filter = new MinFreeMemFilter("mem", props);

    when(executorInfo.getRemainingMemoryInMB()).thenReturn(200L);
    assertThat(filter.filterTarget(executor, flow)).isTrue();

    when(executorInfo.getRemainingMemoryInMB()).thenReturn(30L);
    assertThat(filter.filterTarget(executor, flow)).isFalse();
  }

}

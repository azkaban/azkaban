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
public class MaxCpuUsageFilterTest {

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
    MaxCpuUsageFilter filter = new MaxCpuUsageFilter("cpu", new Props());

    when(executorInfo.getCpuUsage()).thenReturn((double) MaxCpuUsageFilter.DEFAULT_MAX_CPU_USAGE);
    assertThat(filter.filterTarget(executor, flow)).isFalse();

    when(executorInfo.getCpuUsage()).thenReturn((double) MaxCpuUsageFilter.DEFAULT_MAX_CPU_USAGE - 1);
    assertThat(filter.filterTarget(executor, flow)).isTrue();
  }

  @Test
  public void filterShouldUseConfiguredThresholdIfExists() {
    Props props = Props.of(MaxCpuUsageFilter.MAX_CPU_USAGE_LIMIT_CONFIG_KEY, "30");
    MaxCpuUsageFilter filter = new MaxCpuUsageFilter("cpu", props);

    when(executorInfo.getCpuUsage()).thenReturn(10d);
    assertThat(filter.filterTarget(executor, flow)).isTrue();

    when(executorInfo.getCpuUsage()).thenReturn(30d);
    assertThat(filter.filterTarget(executor, flow)).isFalse();
  }

}

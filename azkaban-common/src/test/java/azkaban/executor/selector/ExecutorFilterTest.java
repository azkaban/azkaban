package azkaban.executor.selector;

import azkaban.Constants;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.selector.filter.MinFreeMemFilter;
import azkaban.executor.selector.filter.RemainingFlowSizeFilter;
import azkaban.utils.Props;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ExecutorFilterTest {

  @Test
  public void filterManagerShouldPassRelatedConfigToFilter() {
    Props props = new Props();
    props.put(String.format("%s.%s.%s",
        Constants.ConfigurationKeys.EXECUTOR_SELECTOR_FILTERS,
        ExecutorFilter.STATICREMAININGFLOWSIZE_FILTER_NAME,
        RemainingFlowSizeFilter.MIN_REMAINING_CAPACITY_CONFIG_KEY
    ), "10");
    props.put(String.format("%s.%s.%s",
        Constants.ConfigurationKeys.EXECUTOR_SELECTOR_FILTERS,
        ExecutorFilter.MINIMUMFREEMEMORY_FILTER_NAME,
        MinFreeMemFilter.MIN_FREE_MEM_IN_MB_CONFIG_KEY
    ), "100");
    ExecutorFilter executorFilter = new ExecutorFilter(props, Arrays.asList(
        ExecutorFilter.STATICREMAININGFLOWSIZE_FILTER_NAME,
        ExecutorFilter.MINIMUMFREEMEMORY_FILTER_NAME
        ));
    Map<String, FactorFilter<Executor, ExecutableFlow>> filters = executorFilter.getFactorFilters();
    assertThat(filters).hasSize(2);
    FactorFilter<Executor, ExecutableFlow> flowFilter =
        filters.get(ExecutorFilter.STATICREMAININGFLOWSIZE_FILTER_NAME);
    assertThat(flowFilter.getFilterProps()
        .getInt(RemainingFlowSizeFilter.MIN_REMAINING_CAPACITY_CONFIG_KEY)
    ).isEqualTo(10);
    FactorFilter<Executor, ExecutableFlow> memFilter =
        filters.get(ExecutorFilter.MINIMUMFREEMEMORY_FILTER_NAME);
    assertThat(memFilter.getFilterProps()
        .getInt(MinFreeMemFilter.MIN_FREE_MEM_IN_MB_CONFIG_KEY)
    ).isEqualTo(100);
  }

}

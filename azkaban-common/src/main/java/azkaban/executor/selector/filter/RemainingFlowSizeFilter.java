package azkaban.executor.selector.filter;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorInfo;
import azkaban.executor.selector.FactorFilter;
import azkaban.utils.Props;

/**
 * Filter that checks target remaining capacity to ensure we do not overload it
 */
public class RemainingFlowSizeFilter extends FactorFilter<Executor, ExecutableFlow> {

  public static final int DEFAULT_MIN_REMAINING_CAPACITY = 0;
  public static final String MIN_REMAINING_CAPACITY_CONFIG_KEY = "minRemainingCapacity";

  public RemainingFlowSizeFilter(String factorName, Props filterProps) {
    super(factorName, filterProps);
  }

  @Override
  public boolean filterTarget(Executor filteringTarget, ExecutableFlow referencingObject) {
    if (filteringTarget == null) {
      getLogger().warn(String.format("%s : filtering out the target as it is null.",
          getFactorName()));
      return false;
    }

    final ExecutorInfo stats = filteringTarget.getExecutorInfo();
    if (stats == null) {
      getLogger().warn(String.format("%s : filtering out %s as it's stats is unavailable.",
          getFactorName(),
          filteringTarget.toString()));
      return false;
    }
    int minRemaining = getFilterProps().getInt(MIN_REMAINING_CAPACITY_CONFIG_KEY,
        DEFAULT_MIN_REMAINING_CAPACITY);
    return stats.getRemainingFlowCapacity() > minRemaining;
  }

}

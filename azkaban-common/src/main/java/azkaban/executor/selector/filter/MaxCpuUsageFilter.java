package azkaban.executor.selector.filter;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorInfo;
import azkaban.executor.selector.FactorFilter;
import azkaban.utils.Props;

/**
 * Filter that checks target current cpu usage to ensure we do not overload it
 */
public class MaxCpuUsageFilter extends FactorFilter<Executor, ExecutableFlow> {
  public static final int DEFAULT_MAX_CPU_USAGE = 95;
  public static final String MAX_CPU_USAGE_LIMIT_CONFIG_KEY = "maxCpuUsagePercent";

  public MaxCpuUsageFilter(String factorName, Props filterProps) {
    super(factorName, filterProps);
  }

  @Override
  public boolean filterTarget(Executor filteringTarget, ExecutableFlow referencingObject) {
    if (filteringTarget == null) {
      getLogger().warn(String
          .format("%s : filtering out the target as it is null.", getFactorName()));
      return false;
    }

    final ExecutorInfo stats = filteringTarget.getExecutorInfo();
    if (null == stats) {
      getLogger().warn(String.format("%s : filtering out %s as it's stats is unavailable.",
          getFactorName(),
          filteringTarget.toString()));
      return false;
    }
    int maxCpuUsageLimit = getFilterProps().getInt(MAX_CPU_USAGE_LIMIT_CONFIG_KEY,
        DEFAULT_MAX_CPU_USAGE);
    return stats.getCpuUsage() < maxCpuUsageLimit;
  }
}

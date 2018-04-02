package azkaban.executor.selector.filter;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorInfo;
import azkaban.executor.selector.FactorFilter;
import azkaban.utils.Props;

/**
 * Filter that checks target remaining free memory to ensure we do not overload it
 */
public class MinFreeMemFilter extends FactorFilter<Executor, ExecutableFlow> {
  // Note (sxu): This default value doesn't quite make sense as an OSS project.
  //             But I'm gonna leave it here to keep backward compatibility if any.
  public static final long DEFAULT_MIN_FREE_MEM_IN_MB = 6 * 1024L;
  public static final String MIN_FREE_MEM_IN_MB_CONFIG_KEY = "minFreeMemoryInMB";

  public MinFreeMemFilter(String factorName, Props filterProps) {
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
    long minFreeMem = getFilterProps().getLong(MIN_FREE_MEM_IN_MB_CONFIG_KEY,
        DEFAULT_MIN_FREE_MEM_IN_MB);
    return stats.getRemainingMemoryInMB() > minFreeMem;
  }
}

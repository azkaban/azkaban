/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor.selector;

import azkaban.Constants;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.selector.filter.MaxCpuUsageFilter;
import azkaban.executor.selector.filter.MinFreeMemFilter;
import azkaban.executor.selector.filter.RemainingFlowSizeFilter;
import azkaban.utils.Props;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * De-normalized version of the candidateFilter, which also contains the implementation of the
 * factor filters.
 */
public final class ExecutorFilter extends CandidateFilter<Executor, ExecutableFlow> {

  // factor filter names.
  public static final String STATICREMAININGFLOWSIZE_FILTER_NAME = "StaticRemainingFlowSize";
  public static final String MINIMUMFREEMEMORY_FILTER_NAME = "MinimumFreeMemory";
  public static final String CPUSTATUS_FILTER_NAME = "CpuStatus";
  private static Map<String, Function<Props, FactorFilter<Executor, ExecutableFlow>>> filterBuilders = new HashMap<>();

  static {
    filterBuilders.put(STATICREMAININGFLOWSIZE_FILTER_NAME,
        (filterProps) -> new RemainingFlowSizeFilter(STATICREMAININGFLOWSIZE_FILTER_NAME, filterProps));
    filterBuilders.put(MINIMUMFREEMEMORY_FILTER_NAME,
        (filterProps) -> new MinFreeMemFilter(MINIMUMFREEMEMORY_FILTER_NAME, filterProps));
    filterBuilders.put(CPUSTATUS_FILTER_NAME,
        (filterProps) -> new MaxCpuUsageFilter(CPUSTATUS_FILTER_NAME, filterProps));
  }

  /**
   * constructor of the ExecutorFilter.
   *
   * @param filterList the list of filter to be registered, the parameter must be a not-empty and
   * valid list object.
   */
  public ExecutorFilter(final Props azkProps, final Collection<String> filterList) {
    // shortcut if the filter list is invalid. A little bit ugly to have to throw in constructor.
    if (null == filterList || filterList.size() == 0) {
      logger.error(
          "failed to initialize executor filter as the passed filter list is invalid or empty.");
      throw new IllegalArgumentException("filterList");
    }

    // register the filters according to the list.
    for (final String filterName : filterList) {
      if (filterBuilders.containsKey(filterName)) {
        String filterPropPrefix = String.format("%s.%s.",
            Constants.ConfigurationKeys.EXECUTOR_SELECTOR_FILTERS,
            filterName);
        Props filterProps = azkProps.getProps(filterPropPrefix);
        this.registerFactorFilter(filterBuilders.get(filterName).apply(filterProps));
      } else {
        logger.error(String.format("failed to initialize executor filter " +
                "as the filter implementation for requested factor '%s' doesn't exist.",
            filterName));
        throw new IllegalArgumentException("filterList");
      }
    }
  }

  /**
   * Gets the name list of all available filters.
   *
   * @return the list of the names.
   */
  public static Set<String> getAvailableFilterNames() {
    return filterBuilders.keySet();
  }

  @Override
  public String getName() {
    return "ExecutorFilter";
  }
}

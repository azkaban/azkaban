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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.Statistics;


public final class ExecutorFilter extends CandidateFilter<Executor, ExecutableFlow> {
  private static Map<String, FactorFilter<Executor, ExecutableFlow>> filterRepository = null;

  /**
   * Gets the name list of all available filters.
   * @return the list of the names.
   * */
  public static Set<String> getAvailableFilterNames(){
    return filterRepository.keySet();
  }


  // factor filter names.
  private static final String STATICREMAININGFLOWSIZE_FILTER_NAME = "StaticRemainingFlowSize";

  /**
   * static initializer of the class.
   * We will build the filter repository here.
   * when a new filter is added, please do remember to register it here.
   * */
  static {
    filterRepository = new HashMap<String, FactorFilter<Executor, ExecutableFlow>>();
    filterRepository.put(STATICREMAININGFLOWSIZE_FILTER_NAME, getStaticRemainingFlowSizeFilter());
  }

  /**
   * constructor of the ExecutorFilter.
   * @param filterList   the list of filter to be registered, the parameter must be a not-empty and valid list object.
   * */
  public ExecutorFilter(List<String> filterList) {
    // shortcut if the filter list is invalid. A little bit ugly to have to throw in constructor.
    if (null == filterList || filterList.size() == 0){
      logger.error("failed to initialize executor filter as the passed filter list is invalid or empty.");
      throw new IllegalArgumentException("filterList");
    }

    // register the filters according to the list.
    for (String filterName : filterList){
      if (filterRepository.containsKey(filterName)){
        this.registerFactorFilter(filterRepository.get(filterName));
      } else {
        logger.error(String.format("failed to initialize executor filter as the filter implementation for requested factor '%s' doesn't exist.",
            filterName));
        throw new IllegalArgumentException("filterList");
      }
    }
  }

  @Override
  public String getName() {
    return "ExecutorFilter";
  }

  /**
   * function to register the static remaining flow size filter.
   * NOTE : this is a static filter which means the filter will be filtering based on the system standard which is not
   *        Coming for the passed flow.
   *        Ideally this filter will make sure only the executor has remaining
   * */
  private static FactorFilter<Executor, ExecutableFlow> getStaticRemainingFlowSizeFilter(){
    return FactorFilter.create(STATICREMAININGFLOWSIZE_FILTER_NAME, new FactorFilter.Filter<Executor, ExecutableFlow>() {

      @Override
      public boolean filterTarget(Executor filteringTarget, ExecutableFlow referencingObject) {
        if (null == filteringTarget){
          logger.info(String.format("%s : filtering out the target as it is null.", STATICREMAININGFLOWSIZE_FILTER_NAME));
          return false;
        }

        Statistics stats = filteringTarget.getExecutorStats();
        if (null == stats) {
          logger.info(String.format("%s : filtering out %s as it's stats is unavailable.",
              STATICREMAININGFLOWSIZE_FILTER_NAME,
              filteringTarget.toString()));
          return false;
        }
        return stats.getRemainingFlowCapacity() > 0 ;
       }
    });
  }

  // TO-DO
  // Add more Filter definitions .
}

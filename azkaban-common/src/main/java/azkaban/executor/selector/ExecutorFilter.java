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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorInfo;

/**
 * De-normalized version of the candidateFilter, which also contains the implementation of the factor filters.
 * */
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
  private static final String MINIMUMFREEMEMORY_FILTER_NAME = "MinimumFreeMemory";
  private static final String CPUSTATUS_FILTER_NAME = "CpuStatus";

  /**<pre>
   * static initializer of the class.
   * We will build the filter repository here.
   * when a new filter is added, please do remember to register it here.
   * </pre>
   * */
  static {
    filterRepository = new HashMap<String, FactorFilter<Executor, ExecutableFlow>>();
    filterRepository.put(STATICREMAININGFLOWSIZE_FILTER_NAME, getStaticRemainingFlowSizeFilter());
    filterRepository.put(MINIMUMFREEMEMORY_FILTER_NAME, getMinimumReservedMemoryFilter());
    filterRepository.put(CPUSTATUS_FILTER_NAME, getCpuStatusFilter());
  }

  /**
   * constructor of the ExecutorFilter.
   * @param filterList   the list of filter to be registered, the parameter must be a not-empty and valid list object.
   * */
  public ExecutorFilter(Collection<String> filterList) {
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
        logger.error(String.format("failed to initialize executor filter "+
                                   "as the filter implementation for requested factor '%s' doesn't exist.",
                                   filterName));
        throw new IllegalArgumentException("filterList");
      }
    }
  }

  @Override
  public String getName() {
    return "ExecutorFilter";
  }

  /**<pre>
   * function to register the static remaining flow size filter.
   * NOTE : this is a static filter which means the filter will be filtering based on the system standard which is not
   *        Coming for the passed flow.
   *        Ideally this filter will make sure only the executor hasn't reached the Max allowed # of executing flows.
   *</pre>
   * */
  private static FactorFilter<Executor, ExecutableFlow> getStaticRemainingFlowSizeFilter(){
    return FactorFilter.create(STATICREMAININGFLOWSIZE_FILTER_NAME, new FactorFilter.Filter<Executor, ExecutableFlow>() {
      public boolean filterTarget(Executor filteringTarget, ExecutableFlow referencingObject) {
        if (null == filteringTarget){
          logger.debug(String.format("%s : filtering out the target as it is null.", STATICREMAININGFLOWSIZE_FILTER_NAME));
          return false;
        }

        ExecutorInfo stats = filteringTarget.getExecutorInfo();
        if (null == stats) {
          logger.debug(String.format("%s : filtering out %s as it's stats is unavailable.",
              STATICREMAININGFLOWSIZE_FILTER_NAME,
              filteringTarget.toString()));
          return false;
        }
        return stats.getRemainingFlowCapacity() > 0 ;
       }
    });
  }

  /**<pre>
   * function to register the static Minimum Reserved Memory filter.
   * NOTE : this is a static filter which means the filter will be filtering based on the system standard which is not
   *        Coming for the passed flow.
   *        This filter will filter out any executors that has the remaining  memory below 6G
   *</pre>
   * */
  private static FactorFilter<Executor, ExecutableFlow> getMinimumReservedMemoryFilter(){
    return FactorFilter.create(MINIMUMFREEMEMORY_FILTER_NAME, new FactorFilter.Filter<Executor, ExecutableFlow>() {
      private static final int MINIMUM_FREE_MEMORY = 6 * 1024;
      public boolean filterTarget(Executor filteringTarget, ExecutableFlow referencingObject) {
        if (null == filteringTarget){
          logger.debug(String.format("%s : filtering out the target as it is null.", MINIMUMFREEMEMORY_FILTER_NAME));
          return false;
        }

        ExecutorInfo stats = filteringTarget.getExecutorInfo();
        if (null == stats) {
          logger.debug(String.format("%s : filtering out %s as it's stats is unavailable.",
              MINIMUMFREEMEMORY_FILTER_NAME,
              filteringTarget.toString()));
          return false;
        }
        return stats.getRemainingMemoryInMB() > MINIMUM_FREE_MEMORY ;
       }
    });
  }


  /**
   * <pre>
   * function to register the static Minimum Reserved Memory filter.
   * NOTE :  this is a static filter which means the filter will be filtering based on the system standard which
   *        is not Coming for the passed flow.
   *        This filter will filter out any executors that the current CPU usage exceed 95%
   * </pre>
   * */
  private static FactorFilter<Executor, ExecutableFlow> getCpuStatusFilter(){
    return FactorFilter.create(CPUSTATUS_FILTER_NAME, new FactorFilter.Filter<Executor, ExecutableFlow>() {
      private static final int MAX_CPU_CURRENT_USAGE = 95;
      public boolean filterTarget(Executor filteringTarget, ExecutableFlow referencingObject) {
        if (null == filteringTarget){
          logger.debug(String.format("%s : filtering out the target as it is null.", CPUSTATUS_FILTER_NAME));
          return false;
        }

        ExecutorInfo stats = filteringTarget.getExecutorInfo();
        if (null == stats) {
          logger.debug(String.format("%s : filtering out %s as it's stats is unavailable.",
              MINIMUMFREEMEMORY_FILTER_NAME,
              filteringTarget.toString()));
          return false;
        }
        return stats.getCpuUsage() < MAX_CPU_CURRENT_USAGE ;
       }
    });
  }
}

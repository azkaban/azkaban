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

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import azkaban.executor.Executor;
import azkaban.executor.Statistics;

public class ExecutorComparator extends CandidateComparator<Executor> {
  private static Map<String, ComparatorCreator> comparatorCreatorRepository = null;

  /**
   * Gets the name list of all available comparators.
   * @return the list of the names.
   * */
  public static Set<String> getAvailableComparatorNames(){
    return comparatorCreatorRepository.keySet();
  }

  // factor comparator names
  private static final String REMAININGFLOWSIZE_COMPARATOR_NAME = "RemainingFlowSize";
  private static final String MEMORY_COMPARATOR_NAME = "Memory";
  private static final String REMAININGTMPSIZE_COMPARATOR_NAME = "RemainingTmpSize";
  private static final String PRIORITY_COMPARATOR_NAME = "Priority";
  private static final String LSTDISPATCHED_COMPARATOR_NAME = "LastDispatched";
  private static final String CPUUSAGE_COMPARATOR_NAME = "CpuUsage";

  /**
   * static initializer of the class.
   * We will build the filter repository here.
   * when a new comparator is added, please do remember to register it here.
   * */
  static {
    comparatorCreatorRepository = new HashMap<String, ComparatorCreator>();

    // register the creator for remaining flow size comparator.
    comparatorCreatorRepository.put(REMAININGFLOWSIZE_COMPARATOR_NAME, new ComparatorCreator(){
      @Override public FactorComparator<Executor> create(int weight) { return getRemainingFlowSizeComparator(weight); }});

    // register the creator for memory comparator.
    comparatorCreatorRepository.put(MEMORY_COMPARATOR_NAME, new ComparatorCreator(){
      @Override public FactorComparator<Executor> create(int weight) { return getMemoryComparator(weight); }});

    // register the creator for priority comparator.
    comparatorCreatorRepository.put(PRIORITY_COMPARATOR_NAME, new ComparatorCreator(){
      @Override public FactorComparator<Executor> create(int weight) { return getPriorityComparator(weight); }});

    // register the creator for remaining TMP size comparator.
    comparatorCreatorRepository.put(PRIORITY_COMPARATOR_NAME, new ComparatorCreator(){
      @Override public FactorComparator<Executor> create(int weight) { return getRemainingTmpSizeComparator(weight); }});

    // register the creator for last dispatched time comparator.
    comparatorCreatorRepository.put(LSTDISPATCHED_COMPARATOR_NAME, new ComparatorCreator(){
      @Override public FactorComparator<Executor> create(int weight) { return getLstDispatchedTimeComparator(weight); }});

    // register the creator for CPU Usage comparator.
    comparatorCreatorRepository.put(CPUUSAGE_COMPARATOR_NAME, new ComparatorCreator(){
      @Override public FactorComparator<Executor> create(int weight) { return getCpuUsageComparator(weight); }});
  }


  /**
   * constructor of the ExecutorComparator.
   * @param comparatorList   the list of comparator, plus its weight information to be registered,
   *  the parameter must be a not-empty and valid list object.
   * */
  public ExecutorComparator(Map<String,Integer> comparatorList) {
    if (null == comparatorList|| comparatorList.size() == 0){
      logger.error("failed to initialize executor comparator as the passed comparator list is invalid or empty.");
      throw new IllegalArgumentException("filterList");
    }

    // register the comparators, we will now throw here if the weight is invalid, it is handled in the super.
    for (Entry<String,Integer> entry : comparatorList.entrySet()){
      if (comparatorCreatorRepository.containsKey(entry.getKey())){
        this.registerFactorComparator(comparatorCreatorRepository.
            get(entry.getKey()).
            create(entry.getValue()));
      } else {
        logger.error(String.format("failed to initialize executor comparator as the comparator implementation for requested factor '%s' doesn't exist.",
            entry.getKey()));
        throw new IllegalArgumentException("comparatorList");
      }
    }
  }

  @Override
  public String getName() {
    return "ExecutorComparator";
  }

  private interface ComparatorCreator{
    FactorComparator<Executor> create(int weight);
  }

  /**
   * helper function that does the object  on two statistics, comparator can leverage this function to provide
   * shortcuts if   the statistics object is missing from one or both sides of the executors.
   * @param stat1   the first statistics  object to be checked .
   * @param stat2   the second statistics object to be checked.
   * @param caller  the name of the calling function, for logging purpose.
   * @param result  result Integer to pass out the result in case the statistics are not both valid.
   * @return true if the passed statistics are NOT both valid, a shortcut can be made (caller can consume the result),
   *         false otherwise.
   * */
  private static boolean statisticsObjectCheck(Statistics statisticsObj1, Statistics statisticsObj2, String caller, Integer result){
    result = 0 ;
    // both doesn't expose the info
    if (null == statisticsObj1 && null == statisticsObj2){
      logger.info(String.format("%s : neither of the executors exposed statistics info.",
          caller));
      return true;
    }

    //right side doesn't expose the info.
    if (null == statisticsObj2 ){
        logger.info(String.format("%s : choosing left side and the right side executor doesn't expose statistics info",
            caller));
        result = 1;
        return true;
    }

    //left side doesn't expose the info.
    if (null == statisticsObj1 ){
      logger.info(String.format("%s : choosing right side and the left side executor doesn't expose statistics info",
          caller));
      result = -1;
      return true;
      }

    // both not null
    return false;
  }

  /**
   * function defines the remaining flow size comparator.
   * @param weight weight of the comparator.
   * */
  private static FactorComparator<Executor> getRemainingFlowSizeComparator(int weight){
    return FactorComparator.create(REMAININGFLOWSIZE_COMPARATOR_NAME, weight, new Comparator<Executor>(){

      @Override
      public int compare(Executor o1, Executor o2) {
        Statistics stat1 = o1.getExecutorStats();
        Statistics stat2 = o2.getExecutorStats();

        Integer result = 0;
        if (statisticsObjectCheck(stat1,stat2,REMAININGFLOWSIZE_COMPARATOR_NAME,result)){
          return result;
        }
        return ((Integer)stat1.getRemainingFlowCapacity()).compareTo(stat2.getRemainingFlowCapacity());
      }});
  }

  /**
   * function defines the remaining folder size comparator.
   * @param weight weight of the comparator.
   * */
  private static FactorComparator<Executor> getRemainingTmpSizeComparator(int weight){
    return FactorComparator.create(REMAININGTMPSIZE_COMPARATOR_NAME, weight, new Comparator<Executor>(){

      @Override
      public int compare(Executor o1, Executor o2) {
        Statistics stat1 = o1.getExecutorStats();
        Statistics stat2 = o2.getExecutorStats();

        Integer result = 0;
        if (statisticsObjectCheck(stat1,stat2,REMAININGTMPSIZE_COMPARATOR_NAME,result)){
          return result;
        }
        return ((Long)stat1.getRemainingStorage()).compareTo(stat2.getRemainingStorage());
      }});
  }

  /**
   * function defines the priority comparator.
   * @param weight weight of the comparator.
   * @return
   * */
  private static FactorComparator<Executor> getPriorityComparator(int weight){
    return FactorComparator.create(PRIORITY_COMPARATOR_NAME, weight, new Comparator<Executor>(){

      @Override
      public int compare(Executor o1, Executor o2) {
        Statistics stat1 = o1.getExecutorStats();
        Statistics stat2 = o2.getExecutorStats();

        Integer result = 0;
        if (statisticsObjectCheck(stat1,stat2,PRIORITY_COMPARATOR_NAME,result)){
          return result;
        }
        return ((Integer)stat1.getPriority()).compareTo(stat2.getPriority());
      }});
  }

  /**
   * function defines the cpuUsage comparator.
   * @param weight weight of the comparator.
   * @return
   * */
  private static FactorComparator<Executor> getCpuUsageComparator(int weight){
    return FactorComparator.create(CPUUSAGE_COMPARATOR_NAME, weight, new Comparator<Executor>(){

      @Override
      public int compare(Executor o1, Executor o2) {
        Statistics stat1 = o1.getExecutorStats();
        Statistics stat2 = o2.getExecutorStats();

        Integer result = 0;
        if (statisticsObjectCheck(stat1,stat2,CPUUSAGE_COMPARATOR_NAME,result)){
          return result;
        }
        return ((Double)stat1.getCpuUsage()).compareTo(stat2.getCpuUsage());
      }});
  }


  /**
   * function defines the last dispatched time comparator.
   * @param weight weight of the comparator.
   * @return
   * */
  private static FactorComparator<Executor> getLstDispatchedTimeComparator(int weight){
    return FactorComparator.create(LSTDISPATCHED_COMPARATOR_NAME, weight, new Comparator<Executor>(){

      @Override
      public int compare(Executor o1, Executor o2) {
        Statistics stat1 = o1.getExecutorStats();
        Statistics stat2 = o2.getExecutorStats();

        Integer result = 0;
        if (statisticsObjectCheck(stat1,stat2,LSTDISPATCHED_COMPARATOR_NAME,result)){
          return result;
        }

        if (null == stat1.getLastDispatchedTime() && null == stat1.getLastDispatchedTime()){
          logger.info(String.format("%s : stats from both side doesn't contain last dispatched time info.",
              LSTDISPATCHED_COMPARATOR_NAME));
          return 0;
        }

        if (null == stat2.getLastDispatchedTime()){
          logger.info(String.format("%s : choosing left side as right doesn't contain last dispatched time info.",
              LSTDISPATCHED_COMPARATOR_NAME));
          return 1;
        }

        if (null == stat1.getLastDispatchedTime()){
          logger.info(String.format("%s : choosing right side as left doesn't contain last dispatched time info.",
              LSTDISPATCHED_COMPARATOR_NAME));
          return -1;
        }

        // Note: an earlier date time indicates higher weight.
        return ((Date)stat2.getLastDispatchedTime()).compareTo(stat1.getLastDispatchedTime());
      }});
  }


  /**
   * function defines the Memory comparator.
   * @param weight weight of the comparator.
   * Note: comparator firstly take the absolute value of the remaining memory, if both sides have the same value,
   *       it go further to check the percent of the remaining memory.
   * @return
   * */
  private static FactorComparator<Executor> getMemoryComparator(int weight){
    return FactorComparator.create(MEMORY_COMPARATOR_NAME, weight, new Comparator<Executor>(){

      @Override
      public int compare(Executor o1, Executor o2) {
       Statistics stat1 = o1.getExecutorStats();
       Statistics stat2 = o2.getExecutorStats();

       Integer result = 0;
       if (statisticsObjectCheck(stat1,stat2,MEMORY_COMPARATOR_NAME,result)){
         return result;
       }

       if (stat1.getRemainingMemory() != stat2.getRemainingMemory()){
         return stat1.getRemainingMemory() > stat2.getRemainingMemory() ? 1:-1;
       }

       return Double.compare(stat1.getRemainingMemoryPercent(), stat2.getRemainingMemoryPercent());
      }});
  }
}

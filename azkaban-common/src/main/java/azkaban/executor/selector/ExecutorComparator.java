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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import azkaban.executor.Executor;
import azkaban.executor.ExecutorInfo;


/**
 * De-normalized version of the CandidateComparator, which also contains the implementation of the factor comparators.
 * */
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
  private static final String NUMOFASSIGNEDFLOW_COMPARATOR_NAME = "NumberOfAssignedFlowComparator";
  private static final String MEMORY_COMPARATOR_NAME = "Memory";
  private static final String LSTDISPATCHED_COMPARATOR_NAME = "LastDispatched";
  private static final String CPUUSAGE_COMPARATOR_NAME = "CpuUsage";

  /**
   * static initializer of the class.
   * We will build the filter repository here.
   * when a new comparator is added, please do remember to register it here.
   * */
  static {
    comparatorCreatorRepository = new HashMap<String, ComparatorCreator>();

    // register the creator for number of assigned flow comparator.
    comparatorCreatorRepository.put(NUMOFASSIGNEDFLOW_COMPARATOR_NAME, new ComparatorCreator(){
      public FactorComparator<Executor> create(int weight) { return getNumberOfAssignedFlowComparator(weight); }});

    // register the creator for memory comparator.
    comparatorCreatorRepository.put(MEMORY_COMPARATOR_NAME, new ComparatorCreator(){
      public FactorComparator<Executor> create(int weight) { return getMemoryComparator(weight); }});

    // register the creator for last dispatched time comparator.
    comparatorCreatorRepository.put(LSTDISPATCHED_COMPARATOR_NAME, new ComparatorCreator(){
      public FactorComparator<Executor> create(int weight) { return getLstDispatchedTimeComparator(weight); }});

    // register the creator for CPU Usage comparator.
    comparatorCreatorRepository.put(CPUUSAGE_COMPARATOR_NAME, new ComparatorCreator(){
      public FactorComparator<Executor> create(int weight) { return getCpuUsageComparator(weight); }});
  }


  /**
   * constructor of the ExecutorComparator.
   * @param comparatorList   the list of comparator, plus its weight information to be registered,
   *  the parameter must be a not-empty and valid list object.
   * */
  public ExecutorComparator(Map<String,Integer> comparatorList) {
    if (null == comparatorList|| comparatorList.size() == 0){
      throw new IllegalArgumentException("failed to initialize executor comparator" +
                                         "as the passed comparator list is invalid or empty.");
    }

    // register the comparators, we will now throw here if the weight is invalid, it is handled in the super.
    for (Entry<String,Integer> entry : comparatorList.entrySet()){
      if (comparatorCreatorRepository.containsKey(entry.getKey())){
        this.registerFactorComparator(comparatorCreatorRepository.
            get(entry.getKey()).
            create(entry.getValue()));
      } else {
        throw new IllegalArgumentException(String.format("failed to initialize executor comparator " +
                                        "as the comparator implementation for requested factor '%s' doesn't exist.",
                                        entry.getKey()));
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

  /**<pre>
   * helper function that does the object  on two statistics, comparator can leverage this function to provide
   * shortcuts if   the statistics object is missing from one or both sides of the executors.
   * </pre>
   * @param stat1   the first statistics  object to be checked .
   * @param stat2   the second statistics object to be checked.
   * @param caller  the name of the calling function, for logging purpose.
   * @param result  result Integer to pass out the result in case the statistics are not both valid.
   * @return true if the passed statistics are NOT both valid, a shortcut can be made (caller can consume the result),
   *         false otherwise.
   * */
  private static boolean statisticsObjectCheck(ExecutorInfo statisticsObj1,
                                               ExecutorInfo statisticsObj2, String caller, Integer result){
    result = 0 ;
    // both doesn't expose the info
    if (null == statisticsObj1 && null == statisticsObj2){
      logger.debug(String.format("%s : neither of the executors exposed statistics info.",
          caller));
      return true;
    }

    //right side doesn't expose the info.
    if (null == statisticsObj2 ){
        logger.debug(String.format("%s : choosing left side and the right side executor doesn't expose statistics info",
            caller));
        result = 1;
        return true;
    }

    //left side doesn't expose the info.
    if (null == statisticsObj1 ){
      logger.debug(String.format("%s : choosing right side and the left side executor doesn't expose statistics info",
          caller));
      result = -1;
      return true;
      }

    // both not null
    return false;
  }

  /**
   * function defines the number of assigned flow comparator.
   * @param weight weight of the comparator.
   * */
  private static FactorComparator<Executor> getNumberOfAssignedFlowComparator(int weight){
    return FactorComparator.create(NUMOFASSIGNEDFLOW_COMPARATOR_NAME, weight, new Comparator<Executor>(){

      @Override
      public int compare(Executor o1, Executor o2) {
        ExecutorInfo stat1 = o1.getExecutorInfo();
        ExecutorInfo stat2 = o2.getExecutorInfo();

        Integer result = 0;
        if (statisticsObjectCheck(stat1,stat2,NUMOFASSIGNEDFLOW_COMPARATOR_NAME,result)){
          return result;
        }
        return ((Integer)stat1.getRemainingFlowCapacity()).compareTo(stat2.getRemainingFlowCapacity());
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
        ExecutorInfo stat1 = o1.getExecutorInfo();
        ExecutorInfo stat2 = o2.getExecutorInfo();

        int result = 0;
        if (statisticsObjectCheck(stat1,stat2,CPUUSAGE_COMPARATOR_NAME,result)){
          return result;
        }

        // CPU usage , the lesser the value is, the better.
        return ((Double)stat2.getCpuUsage()).compareTo(stat1.getCpuUsage());
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
        ExecutorInfo stat1 = o1.getExecutorInfo();
        ExecutorInfo stat2 = o2.getExecutorInfo();

        int result = 0;
        if (statisticsObjectCheck(stat1,stat2,LSTDISPATCHED_COMPARATOR_NAME,result)){
          return result;
        }
        // Note: an earlier date time indicates higher weight.
        return ((Long)stat2.getLastDispatchedTime()).compareTo(stat1.getLastDispatchedTime());
      }});
  }


  /**<pre>
   * function defines the Memory comparator.
   * Note: comparator firstly take the absolute value of the remaining memory, if both sides have the same value,
   *       it go further to check the percent of the remaining memory.
   * </pre>
   * @param weight weight of the comparator.

   * @return
   * */
  private static FactorComparator<Executor> getMemoryComparator(int weight){
    return FactorComparator.create(MEMORY_COMPARATOR_NAME, weight, new Comparator<Executor>(){

      @Override
      public int compare(Executor o1, Executor o2) {
       ExecutorInfo stat1 = o1.getExecutorInfo();
       ExecutorInfo stat2 = o2.getExecutorInfo();

       int result = 0;
       if (statisticsObjectCheck(stat1,stat2,MEMORY_COMPARATOR_NAME,result)){
         return result;
       }

       if (stat1.getRemainingMemoryInMB() != stat2.getRemainingMemoryInMB()){
         return stat1.getRemainingMemoryInMB() > stat2.getRemainingMemoryInMB() ? 1:-1;
       }

       return Double.compare(stat1.getRemainingMemoryPercent(), stat2.getRemainingMemoryPercent());
      }});
  }
}

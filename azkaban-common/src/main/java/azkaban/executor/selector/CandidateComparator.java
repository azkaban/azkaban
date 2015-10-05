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
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.utils.Pair;

/**
 * <pre>
 *  Abstract class for a candidate comparator.
 *  this class contains implementation of most of the core logics. Implementing classes is expected only to
 *  register factor comparators using the provided register function.
 * <pre>
 */
public abstract class CandidateComparator<T> implements Comparator<T> {
  protected static Logger logger = Logger.getLogger(CandidateComparator.class);

  // internal repository of the registered comparators .
  private Map<String,FactorComparator<T>> factorComparatorList =
      new ConcurrentHashMap<String,FactorComparator<T>>();

  /** gets the name of the current implementation of the candidate comparator.
   * @returns : name of the comparator.
   * */
  public abstract String getName();

  /** tieBreak method which will kick in when the comparator list generated an equality result for
   *  both sides. the tieBreak method will try best to make sure a stable result is returned.
   * */
  protected boolean tieBreak(T object1, T object2){
    if (null == object2) return true;
    if (null == object1) return false;
    return object1.hashCode() >= object2.hashCode();
  }

  /** function to register a factorComparator to the internal Map for future reference.
   * @param factorComparator : the comparator object to be registered.
   * @throws IllegalArgumentException
   * */
  protected void registerFactorComparator(FactorComparator<T> comparator){
      if (null == comparator ||
          Integer.MAX_VALUE - this.getTotalWeight() < comparator.getWeight() ) {
        throw new IllegalArgumentException("unable to register comparator."+
          " The passed comparator is null or has an invalid weight value.");
      }

      // add or replace the Comparator.
      this.factorComparatorList.put(comparator.getFactorName(),comparator);
      logger.debug(String.format("Factor comparator added for '%s'. Weight = '%s'",
          comparator.getFactorName(), comparator.getWeight()));
  }

  /** function returns the total weight of the registered comparators.
   * @return the value of total weight.
   * */
  public int getTotalWeight(){
    int totalWeight = 0 ;

    // save out a copy of the values as HashMap.values() takes o(n) to return the value.
    Collection<FactorComparator<T>> allValues = this.factorComparatorList.values();
    for (FactorComparator<T> item : allValues){
      if (item != null){
        totalWeight += item.getWeight();
      }
    }

    return totalWeight;
  }

  /**
   * <pre>
   * function to actually calculate the scores for the two objects that are being compared.
   *  the comparison follows the following logic -
   *  1. if both objects are equal return 0 score for both.
   *  2. if one side is null, the other side gets all the score.
   *  3. if both sides are non-null value, both values will be passed to all the registered FactorComparators
   *     each factor comparator will generate a result based off it sole logic the weight of the comparator will be
   *     added to the wining side, if equal, no value will be added to either side.
   *  4. final result will be returned in a Pair container.
   *
   * </pre>
   * @param object1  the first  object (left side)  to be compared.
   * @param object2  the second object (right side) to be compared.
   * @return a pair structure contains the score for both sides.
   * */
  public Pair<Integer,Integer> getComparisonScore(T object1, T object2){
    logger.debug(String.format("start comparing '%s' with '%s',  total weight = %s ",
        object1 == null ? "(null)" : object1.toString(),
        object2 == null ? "(null)" : object2.toString(),
        this.getTotalWeight()));

    int result1 = 0 ;
    int result2 = 0 ;

    // short cut if object equals.
    if (object1 ==  object2){
      logger.debug("[Comparator] same object.");
    } else
    // left side is null.
    if (object1 == null){
      logger.debug("[Comparator] left side is null, right side gets total weight.");
      result2 = this.getTotalWeight();
    } else
    // right side is null.
    if (object2 == null){
      logger.debug("[Comparator] right side is null, left side gets total weight.");
      result1 = this.getTotalWeight();
    } else
    // both side is not null,put them thru the full loop
    {
      Collection<FactorComparator<T>> comparatorList = this.factorComparatorList.values();
      for (FactorComparator<T> comparator :comparatorList){
        int result = comparator.compare(object1, object2);
        result1  = result1 + (result > 0 ? comparator.getWeight() : 0);
        result2  = result2 + (result < 0 ? comparator.getWeight() : 0);
        logger.debug(String.format("[Factor: %s] compare result : %s (current score %s vs %s)",
            comparator.getFactorName(), result, result1, result2));
      }
    }
    // in case of same score, use tie-breaker to stabilize the result.
    if (result1 == result2){
      boolean result = this.tieBreak(object1, object2);
      logger.debug("[TieBreaker] TieBreaker chose " +
      (result? String.format("left side (%s)",  null== object1 ? "null": object1.toString()) :
               String.format("right side (%s)", null== object2 ? "null": object2.toString()) ));
      if (result) result1++; else result2++;
    }

    logger.debug(String.format("Result : %s vs %s ",result1,result2));
    return new Pair<Integer,Integer>(result1,result2);
  }

  @Override
  public int compare(T o1, T o2) {
    Pair<Integer,Integer> result = this.getComparisonScore(o1,o2);
    return result.getFirst() == result.getSecond() ? 0 :
                                result.getFirst() > result.getSecond() ? 1 : -1;
  }
}

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

package azkaban.executor.dispatcher;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.utils.Pair;

/** Abstract class for a candidate comparator.
 *  this class contains implementation of most of the core logics. Implementing classes is expected only to
 *  register factor comparators using the provided register function.
 *
 */
public abstract class CandidateComparator<T> implements Comparator<T> {
  private static Logger logger = Logger.getLogger(CandidateComparator.class);

  // internal repository of the registered comparators .
  private Map<String,FactorComparator<T>> factorComparatorList =
      new ConcurrentHashMap<String,FactorComparator<T>>();

  /** gets the name of the current implementation of the candidate comparator.
   * @returns : name of the comparator.
   * */
  public abstract String getName();

  /** function to register a factorComparator to the internal Map for future reference.
   * @param factorComparator : the comparator object to be registered.
   * */
  protected void registerFactorComparator(FactorComparator<T> comparator){
      if (null == comparator ||
          Integer.MAX_VALUE - this.getTotalWeight() < comparator.getWeight() ) {
        logger.info("skipping registerFactorComparator as the comaractor is null or has an invalid weight value.");
        return;
      }

      // add or replace the Comparator.
      this.factorComparatorList.put(comparator.getFactorName(),comparator);
      logger.info(String.format("Factor comparator added for '%s'. Weight = '%s'",
          comparator.getFactorName(), comparator.getWeight()));
  }


  /** function update the weight of a specific registered factorCompartor.
   * @param factorName : the name of the registered factorComparator to adjust.
   * @param weight:      the new weight value to be adjusted to.
   * @return -1 if the factor doesn't exist or the weight value specified is invalid,
   *          the original value before update otherwise.
   * */
  public int adjustFactorWeight(String factorName, int weight){
    // shortcut if the input is invalid.
    if (factorName == null ||
        factorName == "" ||
        weight < 0 ||
        Integer.MAX_VALUE - this.getTotalWeight() < weight){
      logger.info("skipping adjustFactorWeight as one or more of the input parameters are invalid");
      return -1;
    }

    FactorComparator<T> value = this.factorComparatorList.get(factorName);

    // shortcut if the key doesn't exist.
    if (null == value){
      logger.info(String.format("unable to udpate weight as the specified factorName %s doesn't exist",factorName));
      return -1;
    }

    int returnVal = value.getWeight();
    value.updateWeight(weight);
    return returnVal;
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

  /** function to actually calculate the scores for the two objects that are being compared.
   *  the comparison follows the following logic -
   *  1. if both objects are equal return 0 score for both.
   *  2. if one side is null, the other side gets all the score.
   *  3. if both sides are non-null value, both values will be passed to all the registered FactorComparators
   *     each factor comparator will generate a result based off it sole logic the weight of the comparator will be
   *     added to the wining side, if equal, no value will be added to either side.
   *  4. final result will be returned in a Pair container.
   *
   * */
  public Pair<Integer,Integer> getReult(T object1, T object2){
    logger.info(String.format("start comparing '%s' with '%s',  total weight = %s ",
        object1 == null ? "(null)" : object1.toString(),
        object2 == null ? "(null)" : object2.toString(),
        this.getTotalWeight()));

    // short cut if object equals.
    if (object1 ==  object2){
      logger.info("Result : 0 vs 0 (equal)");
      return new Pair<Integer,Integer>(0,0);
    }

    // left side is null.
    if (object1 == null){
      logger.info(String.format("Result : 0 vs %s (left is null)",this.getTotalWeight()));
      return new Pair<Integer, Integer>(0,this.getTotalWeight());
    }

    // right side is null.
    if (object2 == null){
      logger.info(String.format("Result : %s vs 0 (right is null)",this.getTotalWeight()));
      return new Pair<Integer, Integer>(this.getTotalWeight(),0);
    }

    // both side is not null,put them thru the full loop
    int result1 = 0 ;
    int result2 = 0 ;
    Collection<FactorComparator<T>> comparatorList = this.factorComparatorList.values();
    Iterator<FactorComparator<T>> mapItr = comparatorList.iterator();
    while (mapItr.hasNext()){
      FactorComparator<T> comparator = (FactorComparator<T>) mapItr.next();
      int result = comparator.compare(object1, object2);
      result1  = result1 + (result > 0 ? comparator.getWeight() : 0);
      result2  = result2 + (result < 0 ? comparator.getWeight() : 0);
      logger.info(String.format("[Factor: %s] compare result : %s (current score %s vs %s)",
          comparator.getFactorName(), result, result1, result2));
    }
    logger.info(String.format("Result : %s vs %s ",result1,result2));
    return new Pair<Integer,Integer>(result1,result2);
  }

  @Override
  public int compare(T o1, T o2) {
    Pair<Integer,Integer> result = this.getReult(o1,o2);
    return result.getFirst() == result.getSecond() ? 0 :
                                result.getFirst() > result.getSecond() ? 1 : -1;
  }
}

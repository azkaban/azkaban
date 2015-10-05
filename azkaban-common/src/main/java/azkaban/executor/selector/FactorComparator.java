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
import org.apache.log4j.Logger;

/** wrapper class for a factor comparator .
 *@param T: the type of the objects to be compared.
 */
public final class FactorComparator<T>{
  private static Logger logger = Logger.getLogger(CandidateComparator.class);

  private String factorName;
  private int weight;
  private Comparator<T> comparator;

  /** private constructor of the class. User will create the instance of the class by calling the static
   *  method provided below.
   * @param factorName : the factor name .
   * @param weight : the weight of the comparator.
   * @param comparator : function to be provided by user on how the comparison should be made.
   * */
  private FactorComparator(String factorName, int weight, Comparator<T> comparator){
    this.factorName = factorName;
    this.weight = weight;
    this.comparator = comparator;
  }

  /** static function to generate an instance of the class.
   *  refer to the constructor for the param definitions.
   * */
  public static <T> FactorComparator<T> create(String factorName, int weight, Comparator<T> comparator){

    if (null == factorName || factorName.length() == 0 || weight < 0 || null == comparator){
      logger.error("failed to create instance of FactorComparator, at least one of the input paramters are invalid");
      return null;
    }

    return new FactorComparator<T>(factorName,weight,comparator);
  }

  // function to return the factor name.
  public String getFactorName(){
    return this.factorName;
  }

  // function to return the weight value.
  public int getWeight(){
    return this.weight;
  }

  // function to return the weight value.
  public void updateWeight(int value){
    this.weight = value;
  }

  // the actual compare function, which will leverage the user defined function.
  public int compare(T object1, T object2){
    return this.comparator.compare(object1, object2);
  }
}

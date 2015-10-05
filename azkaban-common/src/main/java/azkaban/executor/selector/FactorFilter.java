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

import org.apache.log4j.Logger;

/** wrapper class for a factor Filter .
 *@param T: the type of the objects to be compared.
 *@param V: the type of the object to be used for filtering.
 */
public final class FactorFilter<T,V>{
  private static Logger logger = Logger.getLogger(FactorFilter.class);

  private String factorName;
  private Filter<T,V> filter;

  /** private constructor of the class. User will create the instance of the class by calling the static
   *  method provided below.
   * @param factorName : the factor name .
   * @param filter : user defined function specifying how the filtering should be implemented.
   * */
  private FactorFilter(String factorName, Filter<T,V> filter){
    this.factorName = factorName;
    this.filter = filter;
  }

  /** static function to generate an instance of the class.
   *  refer to the constructor for the param definitions.
   * */
  public static <T,V> FactorFilter<T,V> create(String factorName, Filter<T,V> filter){

    if (null == factorName || factorName.length() == 0 || null == filter){
      logger.error("failed to create instance of FactorFilter, at least one of the input paramters are invalid");
      return null;
    }

    return new FactorFilter<T,V>(factorName,filter);
  }

  // function to return the factor name.
  public String getFactorName(){
    return this.factorName;
  }

  // the actual check function, which will leverage the logic defined by user.
  public boolean filterTarget(T filteringTarget, V referencingObject){
    return this.filter.filterTarget(filteringTarget, referencingObject);
  }

  // interface of the filter.
  public interface Filter<T,V>{

    /**function to analyze the target item according to the reference object to decide whether the item should be filtered.
     * @param filteringTarget   object to be checked.
     * @param referencingObject object which contains statistics based on which a decision is made whether
     *                      the object being checked need to be filtered or not.
     * @return true if the check passed, false if check failed, which means the item need to be filtered.
     * */
    public boolean filterTarget(T filteringTarget, V referencingObject);
  }
}

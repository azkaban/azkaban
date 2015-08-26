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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;


/** Abstract class for a candidate filter.
 *  this class contains implementation of most of the core logics. Implementing classes is expected only to
 *  register filters using the provided register function.
 *
 */
public abstract class CandidateFilter<T,V>  {
  private static Logger logger = Logger.getLogger(CandidateFilter.class);

  // internal repository of the registered filters .
  private Map<String,FactorFilter<T,V>> factorFilterList =
      new ConcurrentHashMap<String,FactorFilter<T,V>>();

  /** gets the name of the current implementation of the candidate filter.
   * @returns : name of the filter.
   * */
  public abstract String getName();

  /** function to register a factorFilter to the internal Map for future reference.
   * @param factorfilter : the Filter object to be registered.
   * */
  protected void registerFactorFilter(FactorFilter<T,V> filter){
      if (null == filter ) {
        logger.info("skipping registerFactorFilter as the comaractor is null or has an invalid weight value.");
        return;
      }

      // add or replace the filter.
      this.factorFilterList.put(filter.getFactorName(),filter);
      logger.info(String.format("Factor filter added for '%s'.",
          filter.getFactorName()));
  }

  /** function to get the filtering result.
   * */
  public boolean check(T item, V object){
    logger.info(String.format("start checking '%s' with factor filter for '%s'",
        item == null ? "(null)" : item.toString(),
        this.getName()));

    Collection<FactorFilter<T,V>> filterList = this.factorFilterList.values();
    Iterator<FactorFilter<T,V>> mapItr = filterList.iterator();
    boolean result = true;
    while (mapItr.hasNext()){
      FactorFilter<T,V> filter = (FactorFilter<T,V>) mapItr.next();
      result &= filter.check(item,object);
      logger.info(String.format("[Factor: %s] filter result : %s ",
          filter.getFactorName(), result));
      if (!result){
        break;
      }
    }
    logger.info(String.format("Final checking result : %s ",result));
    return result;
  }
}

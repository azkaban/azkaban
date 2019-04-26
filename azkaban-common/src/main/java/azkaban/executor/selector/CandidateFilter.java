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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class for a candidate filter. this class contains implementation of most of the core
 * logics. Implementing classes is expected only to register filters using the provided register
 * function.
 */
public abstract class CandidateFilter<T, V> {

  protected static Logger LOG = LoggerFactory.getLogger(CandidateFilter.class);

  // internal repository of the registered filters .
  private final Map<String, FactorFilter<T, V>> factorFilterList =
      new ConcurrentHashMap<>();

  /**
   * gets the name of the current implementation of the candidate filter.
   *
   * @return : name of the filter.
   */
  public abstract String getName();

  /**
   * function to register a factorFilter to the internal Map for future reference.
   *
   * @param filter : the Filter object to be registered.
   */
  protected void registerFactorFilter(final FactorFilter<T, V> filter) {
    if (null == filter) {
      throw new IllegalArgumentException("unable to register factor filter. " +
          "The passed comaractor is null or has an invalid weight value.");
    }

    // add or replace the filter.
    this.factorFilterList.put(filter.getFactorName(), filter);
    LOG.debug(String.format("Factor filter added for '%s'.",
        filter.getFactorName()));
  }

  /**
   * function to analyze the target item according to the reference object to decide whether the
   * item should be filtered.
   *
   * @param filteringTarget: object to be checked.
   * @param referencingObject: object which contains statistics based on which a decision is made
   * whether the object being checked need to be filtered or not.
   * @return true if the check passed, false if check failed, which means the item need to be
   * filtered.
   */
  public boolean filterTarget(final T filteringTarget, final V referencingObject) {
    LOG.debug(String.format("start filtering '%s' with factor filter for '%s'",
        filteringTarget == null ? "(null)" : filteringTarget.toString(),
        this.getName()));

    final Collection<FactorFilter<T, V>> filterList = this.factorFilterList.values();
    boolean result = true;
    for (final FactorFilter<T, V> filter : filterList) {
      result &= filter.filterTarget(filteringTarget, referencingObject);
      LOG.debug(String.format("[Factor: %s] filter result : %s ",
          filter.getFactorName(), result));
      if (!result) {
        break;
      }
    }
    LOG.debug(String.format("Final filtering result : %s ", result));
    return result;
  }
}

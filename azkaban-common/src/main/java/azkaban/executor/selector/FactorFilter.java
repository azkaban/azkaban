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

import azkaban.filter.Filter;
import azkaban.utils.Props;
import org.apache.log4j.Logger;

/**
 * Factor filter base. User is supposed to extend this class for implementations.
 *
 * @see azkaban.filter.Filter
 */
public abstract class FactorFilter<T, V> implements Filter<T, V> {

  private final Logger logger = Logger.getLogger(this.getClass());
  private final String factorName;
  private final Props filterProps;

  /**
   * User is supposed to call this constructor in all implementations.
   *
   * @param factorName: the factor name .
   * @param filterProps: the props to be used by the filter
   */
  public FactorFilter(final String factorName, Props filterProps) {
    this.factorName = factorName;
    this.filterProps = filterProps;
  }

  // function to return the factor name.
  public String getFactorName() {
    return this.factorName;
  }

  /**
   * Get the logger for the filter
   *
   * @return the logger associated with this filter
   */
  public Logger getLogger() {
    return logger;
  }

  /**
   * Get props for this filter
   *
   * @return a subset in main azkaban properties corresponding to this filter, never null.
   */
  public Props getFilterProps() {
    return filterProps;
  }
}

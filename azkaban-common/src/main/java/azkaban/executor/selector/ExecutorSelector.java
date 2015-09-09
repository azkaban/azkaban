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

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;

/**<pre>
 * Executor selector class implementation.
 * NOTE: This class is a de-generalized version of the CandidateSelector, which provides a
 *       clean and convenient constructor to take in filter and comparator name list and build
 *       the instance from that.
 *</pre>
 * */
public class ExecutorSelector extends CandidateSelector<Executor, ExecutableFlow> {

  /**
   * Contractor of the class.
   * @param filterList      name list of the filters to be registered,
   *                        filter feature will be disabled if a null value is passed.
   * @param comparatorList  name/weight pair list of the comparators to be registered ,
   *                        again comparator feature is disabled if a null value is passed.
   * */
  public ExecutorSelector(Collection<String> filterList, Map<String,Integer> comparatorList) {
    super(null == filterList || filterList.isEmpty() ?         null : new ExecutorFilter(filterList),
          null == comparatorList || comparatorList.isEmpty() ? null : new ExecutorComparator(comparatorList));
  }
}

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


/**<pre>
 *  Definition of the selector interface.
 *  an implementation of the selector interface provides the functionality
 *  to return a candidate from the candidateList that suits best for the dispatchingObject.
 * </pre>
 *  @param K : type of the candidate.
 *  @param V : type of the dispatching object.
 */
public interface Selector <K extends Comparable<K>,V> {

  /** Function returns the next best suit candidate from the candidateList for the dispatching object.
   *  @param  candidateList : List of the candidates to select from .
   *  @param  dispatchingObject : the object to be dispatched .
   *  @return candidate from the candidate list that suits best for the dispatching object.
   * */
  public K getBest(Collection<K> candidateList, V dispatchingObject);

  /** Function returns the name of the current Dispatcher
   *  @return name of the dispatcher.
   * */
  public String getName();
}

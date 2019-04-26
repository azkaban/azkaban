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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of the CandidateSelector.
 *
 * @param K executor object type.
 * @param V dispatching object type.
 */
public class CandidateSelector<K extends Comparable<K>, V> implements Selector<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(CandidateComparator.class);

  private final CandidateFilter<K, V> filter;
  private final CandidateComparator<K> comparator;

  /**
   * constructor of the class.
   *
   * @param filter CandidateFilter object to be used to perform the candidate filtering.
   * @param comparator CandidateComparator object to be used to find the best suit candidate from
   * the filtered list.
   */
  public CandidateSelector(final CandidateFilter<K, V> filter,
      final CandidateComparator<K> comparator) {
    this.filter = filter;
    this.comparator = comparator;
  }

  @Override
  public K getBest(final Collection<K> candidateList, final V dispatchingObject) {

    // shortcut if the candidateList is empty.
    if (null == candidateList || candidateList.size() == 0) {
      LOG.error("failed to getNext candidate as the passed candidateList is null or empty.");
      return null;
    }

    LOG.debug("start candidate selection logic.");
    LOG.debug(String.format("candidate count before filtering: %s", candidateList.size()));

    // to keep the input untouched, we will form up a new list based off the filtering result.
    Collection<K> filteredList = new ArrayList<>();

    if (null != this.filter) {
      for (final K candidateInfo : candidateList) {
        if (this.filter.filterTarget(candidateInfo, dispatchingObject)) {
          filteredList.add(candidateInfo);
        }
      }
    } else {
      filteredList = candidateList;
      LOG.debug("skipping the candidate filtering as the filter object is not specifed.");
    }

    LOG.debug(String.format("candidate count after filtering: %s", filteredList.size()));
    if (filteredList.size() == 0) {
      LOG.debug("failed to select candidate as the filtered candidate list is empty.");
      return null;
    }

    if (null == this.comparator) {
      LOG.debug(
          "candidate comparator is not specified, default hash code comparator class will be used.");
    }

    // final work - find the best candidate from the filtered list.
    final K executor = Collections.max(filteredList, this.comparator);
    LOG.debug(String.format("candidate selected %s",
        null == executor ? "(null)" : executor.toString()));
    return executor;
  }

  @Override
  public String getName() {
    return "CandidateSelector";
  }
}

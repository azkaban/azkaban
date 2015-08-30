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
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

/** Implementation of the CandidateSelector.
 *  @param K executor object type.
 *  @param V dispatching object type.
 * */
public class CandidateSelector<K,V> implements Selector<K, V> {
  private static Logger logger = Logger.getLogger(CandidateComparator.class);

  private CandidateFilter<K,V> filter;
  private CandidateComparator<K> comparator;

  /**constructor of the class.
   * @param filter CandidateFilter object to be used to perform the candidate filtering.
   * @param comparator CandidateComparator object to be used to find the best suit candidate from the filtered list.
   * */
  public CandidateSelector(CandidateFilter<K,V> filter,
      CandidateComparator<K> comparator){
    this.filter = filter;
    this.comparator = comparator;
  }

  @Override
  public K getBest(List<K> candidateList, V dispatchingObject) {

     // shortcut if the candidateList is empty.
     if ( null == candidateList || candidateList.size() == 0){
       logger.error("failed to getNext candidate as the passed candidateList is null or empty.");
       return null;
     }

     logger.info("start candidate selection logic.");
     logger.info(String.format("candidate count before filtering: %s", candidateList.size()));

     // to keep the input untouched, we will form up a new list based off the filtering result.
     List<K> filteredList = new ArrayList<K>();

     if (null != this.filter){
       for (K candidateInfo : candidateList){
         if (filter.filterTarget(candidateInfo,dispatchingObject)){
           filteredList.add(candidateInfo);
         }
       }
     } else{
       filteredList = candidateList;
       logger.info("skipping the candidate filtering as the filter object is not specifed.");
     }

     logger.info(String.format("candidate count after filtering: %s", filteredList.size()));
     if (filteredList.size() == 0){
       logger.info("failed to select candidate as the filted candidate list is empty.");
       return null;
     }

     // final work - find the best candidate from the filtered list.
     K executor = Collections.max(filteredList,comparator);
     logger.info(String.format("candidate selected %s",
         null == executor ? "(null)" : executor.toString()));
     return executor;
  }

  @Override
  public String getName() {
    return "CandidateSelector";
  }
}

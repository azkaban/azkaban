/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.metric;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import azkaban.utils.Props;


public class InMemoryMetricEmitter implements IMetricEmitter {
  Map<String, LinkedList<InMemoryHistoryNode>> historyListMapping;
  private static final String INMEMORY_METRIC_REPORTER_WINDOW = "azkaban.metric.inmemory.interval";
  private static final String INMEMORY_METRIC_NUM_INSTANCES = "azkaban.metric.inmemory.maxinstances";

  long Interval;
  long numInstances;

  public InMemoryMetricEmitter(Props azkProps) {
    historyListMapping = new HashMap<String, LinkedList<InMemoryHistoryNode>>();
    Interval = azkProps.getLong(INMEMORY_METRIC_REPORTER_WINDOW, 60 * 60 * 24 * 7);
    numInstances = azkProps.getLong(INMEMORY_METRIC_NUM_INSTANCES, 10000);
  }

  @Override
  public void reportMetric(IMetric<?> metric) throws Exception {
    String metricName = metric.getName();
    if (!historyListMapping.containsKey(metricName)) {
      historyListMapping.put(metricName, new LinkedList<InMemoryHistoryNode>());
    }
    synchronized (historyListMapping.get(metricName)) {
      historyListMapping.get(metricName).add(new InMemoryHistoryNode(metric.getValue()));
      cleanUsingTime(metricName, historyListMapping.get(metricName).peekLast().getTimestamp());
    }
  }

  public List<InMemoryHistoryNode> getDrawMetric(String metricName, Date from, Date to) {
    LinkedList<InMemoryHistoryNode> selectedLists = new LinkedList<InMemoryHistoryNode>();
    if (historyListMapping.containsKey(metricName)) {

      // selecting nodes within time frame
      synchronized (historyListMapping.get(metricName)) {
        for (InMemoryHistoryNode node : historyListMapping.get(metricName)) {
          if (node.getTimestamp().after(from) && node.getTimestamp().before(to)) {
            selectedLists.add(node);
          }
          if (node.getTimestamp().after(to)) {
            break;
          }
        }
      }

      // selecting nodes if num of nodes > numInstances
      if (selectedLists.size() > numInstances) {
        double step = (double) selectedLists.size() / numInstances;
        long nextIndex = 0, currentIndex = 0, numSelectedInstances = 1;
        Iterator<InMemoryHistoryNode> ite = selectedLists.iterator();

        while(ite.hasNext()) {
          ite.next();
          if (currentIndex == nextIndex) {
            nextIndex = (long) Math.floor(numSelectedInstances * step + 0.5);
            numSelectedInstances++;
          } else {
            ite.remove();
          }
          currentIndex++;
        }
      }
    }
    cleanUsingTime(metricName, new Date());
    return selectedLists;
  }

  private void cleanUsingTime(String metricName, Date firstAllowedDate) {
    if (historyListMapping.containsKey(metricName) && historyListMapping.get(metricName) != null) {
      synchronized (historyListMapping.get(metricName)) {

        InMemoryHistoryNode firstNode = historyListMapping.get(metricName).peekFirst();
        // removing objects older than Interval time from firstAllowedDate
        while (firstNode != null
            && TimeUnit.MILLISECONDS.toSeconds(firstAllowedDate.getTime() - firstNode.getTimestamp().getTime()) > Interval) {
          historyListMapping.get(metricName).removeFirst();
          firstNode = historyListMapping.get(metricName).peekFirst();
        }
      }
    }
  }
}

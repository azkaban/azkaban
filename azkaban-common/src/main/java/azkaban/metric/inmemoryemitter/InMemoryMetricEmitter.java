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

package azkaban.metric.inmemoryemitter;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import azkaban.metric.IMetric;
import azkaban.metric.IMetricEmitter;
import azkaban.metric.MetricException;
import azkaban.utils.Props;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


/**
 * Metric Emitter which maintains in memory snapshots of the metrics
 * This is also the default metric emitter and used by /stats servlet
 */
public class InMemoryMetricEmitter implements IMetricEmitter {
  protected static final Logger logger = Logger.getLogger(InMemoryMetricEmitter.class);

  /**
   * Data structure to keep track of snapshots
   */
  protected Map<String, LinkedList<InMemoryHistoryNode>> historyListMapping;
  private static final String INMEMORY_METRIC_REPORTER_WINDOW = "azkaban.metric.inmemory.interval";
  private static final String INMEMORY_METRIC_NUM_INSTANCES = "azkaban.metric.inmemory.maxinstances";
  private static final String INMEMORY_METRIC_STANDARDDEVIATION_FACTOR =
      "azkaban.metric.inmemory.standardDeviationFactor";

  private double standardDeviationFactor;
  /**
   * Interval (in millisecond) from today for which we should maintain the in memory snapshots
   */
  private long timeWindow;
  /**
   * Maximum number of snapshots that should be displayed on /stats servlet
   */
  private long numInstances;

  /**
   * @param azkProps Azkaban Properties
   */
  public InMemoryMetricEmitter(Props azkProps) {
    historyListMapping = new HashMap<String, LinkedList<InMemoryHistoryNode>>();
    timeWindow = azkProps.getLong(INMEMORY_METRIC_REPORTER_WINDOW, 60 * 60 * 24 * 7 * 1000);
    numInstances = azkProps.getLong(INMEMORY_METRIC_NUM_INSTANCES, 50);
    standardDeviationFactor = azkProps.getDouble(INMEMORY_METRIC_STANDARDDEVIATION_FACTOR, 2);
  }

  /**
   * Update reporting interval
   * @param val interval in milli seconds
   */
  public synchronized void setReportingInterval(long val) {
    timeWindow = val;
  }

  /**
   * Set number of /stats servlet display points
   * @param num
   */
  public void setReportingInstances(long num) {
    numInstances = num;
  }

  /**
   * Ingest metric in snapshot data structure while maintaining interval
   * {@inheritDoc}
   * @see azkaban.metric.IMetricEmitter#reportMetric(azkaban.metric.IMetric)
   */
  @Override
  public void reportMetric(final IMetric<?> metric) throws MetricException {
    String metricName = metric.getName();
    if (!historyListMapping.containsKey(metricName)) {
      logger.info("First time capturing metric: " + metricName);
      historyListMapping.put(metricName, new LinkedList<InMemoryHistoryNode>());
    }
    synchronized (historyListMapping.get(metricName)) {
      logger.debug("Ingesting metric: " + metricName);
      historyListMapping.get(metricName).add(new InMemoryHistoryNode(metric.getValue()));
      cleanUsingTime(metricName, historyListMapping.get(metricName).peekLast().getTimestamp());
    }
  }

  /**
   * Get snapshots for a given metric at a given time
   * @param metricName name of the metric
   * @param from Start date
   * @param to end date
   * @param useStats get statistically significant points only
   * @return List of snapshots
   */
  public List<InMemoryHistoryNode> getMetrics(final String metricName, final Date from, final Date to,
      final Boolean useStats) throws ClassCastException {
    LinkedList<InMemoryHistoryNode> selectedLists = new LinkedList<InMemoryHistoryNode>();
    if (historyListMapping.containsKey(metricName)) {

      logger.debug("selecting snapshots within time frame");
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
      if (useStats) {
        statBasedSelectMetricHistory(selectedLists);
      } else {
        generalSelectMetricHistory(selectedLists);
      }
    }
    cleanUsingTime(metricName, new Date());
    return selectedLists;
  }

  /**
   * filter snapshots using statistically significant points only
   * @param selectedLists list of snapshots
   */
  private void statBasedSelectMetricHistory(final LinkedList<InMemoryHistoryNode> selectedLists)
      throws ClassCastException {
    logger.debug("selecting snapshots which are far away from mean value");
    DescriptiveStatistics descStats = getDescriptiveStatistics(selectedLists);
    Double mean = descStats.getMean();
    Double std = descStats.getStandardDeviation();

    Iterator<InMemoryHistoryNode> ite = selectedLists.iterator();
    while (ite.hasNext()) {
      InMemoryHistoryNode currentNode = ite.next();
      double value = ((Number) currentNode.getValue()).doubleValue();
      // remove all elements which lies in 95% value band
      if (value < mean + standardDeviationFactor * std && value > mean - standardDeviationFactor * std) {
        ite.remove();
      }
    }
  }

  private DescriptiveStatistics getDescriptiveStatistics(final LinkedList<InMemoryHistoryNode> selectedLists)
      throws ClassCastException {
    DescriptiveStatistics descStats = new DescriptiveStatistics();
    for (InMemoryHistoryNode node : selectedLists) {
      descStats.addValue(((Number) node.getValue()).doubleValue());
    }
    return descStats;
  }

  /**
   * filter snapshots by evenly selecting points across the interval
   * @param selectedLists list of snapshots
   */
  private void generalSelectMetricHistory(final LinkedList<InMemoryHistoryNode> selectedLists) {
    logger.debug("selecting snapshots evenly from across the time interval");
    if (selectedLists.size() > numInstances) {
      double step = (double) selectedLists.size() / numInstances;
      long nextIndex = 0, currentIndex = 0, numSelectedInstances = 1;
      Iterator<InMemoryHistoryNode> ite = selectedLists.iterator();
      while (ite.hasNext()) {
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

  /**
   * Remove snapshots to maintain reporting interval
   * @param metricName Name of the metric
   * @param firstAllowedDate End date of the interval
   */
  private void cleanUsingTime(final String metricName, final Date firstAllowedDate) {
    if (historyListMapping.containsKey(metricName) && historyListMapping.get(metricName) != null) {
      synchronized (historyListMapping.get(metricName)) {

        InMemoryHistoryNode firstNode = historyListMapping.get(metricName).peekFirst();
        long localCopyOfTimeWindow = 0;

        // go ahead for clean up using latest possible value of interval
        // any interval change will not affect on going clean up
        synchronized (this) {
          localCopyOfTimeWindow = timeWindow;
        }

        // removing objects older than Interval time from firstAllowedDate
        while (firstNode != null
            && TimeUnit.MILLISECONDS.toMillis(firstAllowedDate.getTime() - firstNode.getTimestamp().getTime()) > localCopyOfTimeWindow) {
          historyListMapping.get(metricName).removeFirst();
          firstNode = historyListMapping.get(metricName).peekFirst();
        }
      }
    }
  }

  /**
   * Clear snapshot data structure
   * {@inheritDoc}
   * @see azkaban.metric.IMetricEmitter#purgeAllData()
   */
  @Override
  public void purgeAllData() throws MetricException {
    historyListMapping.clear();
  }
}

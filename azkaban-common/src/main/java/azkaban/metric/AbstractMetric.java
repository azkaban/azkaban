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

import org.apache.log4j.Logger;

/**
 * Abstract class for Metric
 * @param <T> Type of Value of a given metric
 */
public abstract class AbstractMetric<T> implements IMetric<T>, Cloneable{
  protected static final Logger logger = Logger.getLogger(MetricReportManager.class);
  protected String name;
  protected T value;
  protected String type;
  protected MetricReportManager metricManager;

  /**
   * @param metricName Name of metric
   * @param metricType Metric type. For display purposes.
   * @param initialValue Initial Value of a metric
   * @param manager Metric Manager whom a metric will report to
   */
  protected AbstractMetric(String metricName, String metricType, T initialValue, MetricReportManager manager) {
    name = metricName;
    type = metricType;
    value = initialValue;
    metricManager = manager;
  }

  /**
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#getName()
   */
  public String getName() {
    return name;
  }

  /**
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#getValueType()
   */
  public String getValueType() {
    return type;
  }

  /**
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#updateMetricManager(azkaban.metric.MetricReportManager)
   */
  public void updateMetricManager(final MetricReportManager manager) {
    metricManager = manager;
  }

  /**
   * {@inheritDoc}
   * @throws CloneNotSupportedException
   * @see azkaban.metric.IMetric#getSnapshot()
   */
  @SuppressWarnings("unchecked")
  public IMetric<T> getSnapshot() throws CloneNotSupportedException{
    return (IMetric<T>) this.clone();
  }

  /**
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#getValue()
   */
  public T getValue() {
    return value;
  }

  /**
   * Method used to notify manager for a tracking event.
   * Metric is free to call this method as per implementation.
   * Timer based or Azkaban events are the most common implementation
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#notifyManager()
   */
  public void notifyManager() {
    logger.debug(String.format("Notifying Manager for %s", this.getClass().getName()));
    try {
      metricManager.reportMetric(this);
    } catch (Throwable ex) {
      logger.error(String.format("Metric Manager is not set for %s metric", this.getClass().getName()), ex);
    }
  }
}

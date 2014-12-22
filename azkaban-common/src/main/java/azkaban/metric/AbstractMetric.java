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
  protected static final Logger _logger = Logger.getLogger(MetricReportManager.class);
  protected String _name;
  protected T _value;
  protected String _type;
  protected MetricReportManager _metricManager;

  /**
   * @param metricName Name of metric
   * @param metricType Metric type. For display purposes.
   * @param initialValue Initial Value of a metric
   * @param manager Metric Manager whom a metric will report to
   */
  protected AbstractMetric(String metricName, String metricType, T initialValue, MetricReportManager manager) {
    _name = metricName;
    _type = metricType;
    _value = initialValue;
    _metricManager = manager;
  }

  /**
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#getName()
   */
  public String getName() {
    return _name;
  }

  /**
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#getValueType()
   */
  public String getValueType() {
    return _type;
  }

  /**
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#updateMetricManager(azkaban.metric.MetricReportManager)
   */
  public void updateMetricManager(final MetricReportManager manager) {
    _metricManager = manager;
  }

  /**
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#getValue()
   */
  public T getValue() {
    return _value;
  }

  /**
   * Method used to notify manager for a tracking event.
   * Metric is free to call this method as per implementation.
   * Timer based or Azkaban events are the most common implementation
   * {@inheritDoc}
   * @see azkaban.metric.IMetric#notifyManager()
   */
  public synchronized void notifyManager() {
    _logger.debug(String.format("Notifying Manager for %s", this.getClass().getName()));
    try {
      _metricManager.reportMetric( (IMetric<?>) this.clone());
    } catch (NullPointerException ex) {
      _logger.error(String.format("Metric Manager is not set for %s metric %s", this.getClass().getName(), ex.toString()));
    } catch (CloneNotSupportedException ex) {
      _logger.error(String.format("Failed to take snapshot for %s metric %s", this.getClass().getName(), ex.toString()));
    }
  }
}

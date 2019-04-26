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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class for Metric
 *
 * @param <T> Type of Value of a given metric
 */
public abstract class AbstractMetric<T> implements IMetric<T>, Cloneable {

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractMetric.class);
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
  protected AbstractMetric(final String metricName, final String metricType, final T initialValue,
      final MetricReportManager manager) {
    this.name = metricName;
    this.type = metricType;
    this.value = initialValue;
    this.metricManager = manager;
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.metric.IMetric#getName()
   */
  @Override
  public String getName() {
    return this.name;
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.metric.IMetric#getValueType()
   */
  @Override
  public String getValueType() {
    return this.type;
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.metric.IMetric#updateMetricManager(azkaban.metric.MetricReportManager)
   */
  @Override
  public void updateMetricManager(final MetricReportManager manager) {
    this.metricManager = manager;
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.metric.IMetric#getSnapshot()
   */
  @Override
  public IMetric<T> getSnapshot() throws CloneNotSupportedException {
    return (IMetric<T>) this.clone();
  }

  /**
   * {@inheritDoc}
   *
   * @see azkaban.metric.IMetric#getValue()
   */
  @Override
  public T getValue() {
    return this.value;
  }

  /**
   * Method used to notify manager for a tracking event. Metric is free to call this method as per
   * implementation. Timer based or Azkaban events are the most common implementation {@inheritDoc}
   *
   * @see azkaban.metric.IMetric#notifyManager()
   */
  @Override
  public void notifyManager() {
    LOG.debug(String.format("Notifying Manager for %s", this.getClass().getName()));
    try {
      this.metricManager.reportMetric(this);
    } catch (final Throwable ex) {
      LOG.error(
          String.format("Metric Manager is not set for %s metric", this.getClass().getName()), ex);
    }
  }
}

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


public abstract class AbstractMetric<T> implements IMetric<T> {
  protected static final Logger logger = Logger.getLogger(MetricReportManager.class);
  protected String name;
  protected T value;
  protected String type;
  protected MetricReportManager metricManager;

  public AbstractMetric(String metricName, String metricType, T initialValue, MetricReportManager manager) {
    name = metricName;
    type = metricType;
    value = initialValue;
    metricManager = manager;
  }

  public String getName() {
    return name;
  }

  public String getValueType() {
    return type;
  }

  public void updateMetricManager(final MetricReportManager manager) {
    metricManager = manager;
  }

  public T getValue() {
    return value;
  }

  public synchronized void notifyManager() {
    logger.debug(String.format("Notifying Manager for %s", this.getClass().getName()));
    try {
      metricManager.reportMetric(this);
    } catch (NullPointerException ex) {
      logger.error(String.format("Metric Manager is not set for %s metric %s", this.getClass().getName(), ex.toString()));
    }
  }
}

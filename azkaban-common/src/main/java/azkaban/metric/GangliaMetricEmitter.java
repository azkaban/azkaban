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

import org.apache.commons.collections.bag.SynchronizedBag;

import azkaban.utils.Props;


/**
 * MetricEmitter implementation to report metric to a ganglia gmetric process
 */
public class GangliaMetricEmitter implements IMetricEmitter {
  private static final String GANGLIA_METRIC_REPORTER_PATH = "azkaban.metric.ganglia.path";
  private String gmetricPath;

  /**
   * @param azkProps Azkaban Properties
   */
  public GangliaMetricEmitter(Props azkProps) {
    gmetricPath = azkProps.get(GANGLIA_METRIC_REPORTER_PATH);
  }

  private String buildCommand(IMetric<?> metric) {
    String cmd = null;

    synchronized (metric) {
      cmd =
          String.format("%s -t %s -n %s -v %s", gmetricPath, metric.getValueType(), metric.getName(), metric.getValue()
              .toString());
    }

    return cmd;
  }

  /**
   * Report metric by executing command line interface of gmetrics
   * {@inheritDoc}
   * @see azkaban.metric.IMetricEmitter#reportMetric(azkaban.metric.IMetric)
   */
  @Override
  public void reportMetric(final IMetric<?> metric) throws MetricException {
    String gangliaCommand = buildCommand(metric);

    if (gangliaCommand != null) {
      // executes shell command to report metric to ganglia dashboard
      try {
        Process emission = Runtime.getRuntime().exec(gangliaCommand);
        int exitCode;
        exitCode = emission.waitFor();
        if (exitCode != 0) {
          throw new MetricException("Failed to report metric using gmetric");
        }
      } catch (Exception e) {
        throw new MetricException("Failed to report metric using gmetric");
      }
    } else {
      throw new MetricException("Failed to build ganglia Command");
    }
  }

  @Override
  public void purgeAllData() throws MetricException {

  }
}

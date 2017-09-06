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

import azkaban.utils.Props;


/**
 * MetricEmitter implementation to report metric to a ganglia gmetric process
 */
public class GangliaMetricEmitter implements IMetricEmitter {

  private static final String GANGLIA_METRIC_REPORTER_PATH = "azkaban.metric.ganglia.path";
  private final String gmetricPath;

  /**
   * @param azkProps Azkaban Properties
   */
  public GangliaMetricEmitter(final Props azkProps) {
    this.gmetricPath = azkProps.get(GANGLIA_METRIC_REPORTER_PATH);
  }

  private String buildCommand(final IMetric<?> metric) {
    String cmd = null;

    synchronized (metric) {
      cmd =
          String
              .format("%s -t %s -n %s -v %s", this.gmetricPath, metric.getValueType(),
                  metric.getName(),
                  metric.getValue()
                      .toString());
    }

    return cmd;
  }

  /**
   * Report metric by executing command line interface of gmetrics {@inheritDoc}
   *
   * @see azkaban.metric.IMetricEmitter#reportMetric(azkaban.metric.IMetric)
   */
  @Override
  public void reportMetric(final IMetric<?> metric) throws MetricException {
    final String gangliaCommand = buildCommand(metric);

    if (gangliaCommand != null) {
      // executes shell command to report metric to ganglia dashboard
      try {
        final Process emission = Runtime.getRuntime().exec(gangliaCommand);
        final int exitCode;
        exitCode = emission.waitFor();
        if (exitCode != 0) {
          throw new MetricException("Failed to report metric using gmetric");
        }
      } catch (final Exception e) {
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

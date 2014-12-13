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


public class GangliaMetricEmitter implements IMetricEmitter {
  private static final String GANGLIA_METRIC_REPORTER_PATH = "azkaban.metric.ganglia.path";

  private String gmetricPath;

  public GangliaMetricEmitter(Props azkProps) {
    gmetricPath = azkProps.get(GANGLIA_METRIC_REPORTER_PATH);
  }

  private String buildCommand(IMetric<?> metric) {
    return String.format("%s -t %s -n %s -v %s", gmetricPath, metric.getValueType(), metric.getName(), metric.getValue().toString());
  }

  @Override
  public void reportMetric(final IMetric<?> metric) throws Exception {
    String gangliaCommand = buildCommand(metric);
    synchronized (metric) {
      // executes shell command to report metric to ganglia dashboard
      Process emission = Runtime.getRuntime().exec(gangliaCommand);
      int exitCode = emission.waitFor();
      if (exitCode != 0) {
        throw new RuntimeException("Failed to report metric using gmetric");
      }
    }
  }
}

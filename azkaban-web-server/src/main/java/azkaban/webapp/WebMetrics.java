/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.webapp;

import azkaban.metrics.MetricsManager;
import azkaban.metrics.MetricsUtility;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This singleton class WebMetrics is in charge of collecting varieties of metrics
 * from azkaban-web-server modules.
 */
public enum WebMetrics {
  INSTANCE;

  private final MetricRegistry registry;

  private final Meter webGetCall;
  private final Meter webPostCall;

  // How long does user log fetch take when user call fetch-log api.
  private final AtomicLong logFetchLatency = new AtomicLong(0L);

  WebMetrics() {
    this.registry = MetricsManager.INSTANCE.getRegistry();
    this.webGetCall = MetricsUtility.addMeter("Web-Get-Call-Meter", this.registry);
    this.webPostCall = MetricsUtility.addMeter("Web-Post-Call-Meter", this.registry);
    MetricsUtility.addGauge("fetchLogLatency", this.registry, this.logFetchLatency::get);
  }

  public void markWebGetCall() {

    /*
     * This method should be Thread Safe.
     * Two reasons that we don't make this function call synchronized:
     * 1). drop wizard metrics deals with concurrency internally;
     * 2). mark is basically a math addition operation, which should not cause race condition issue.
     */
    this.webGetCall.mark();
  }

  public void markWebPostCall() {

    this.webPostCall.mark();
  }

  public void setFetchLogLatency(final long milliseconds) {
    this.logFetchLatency.set(milliseconds);
  }
}

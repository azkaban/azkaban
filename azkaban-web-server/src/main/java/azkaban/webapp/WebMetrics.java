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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;


/**
 * This singleton class WebMetrics is in charge of collecting varieties of metrics
 * from azkaban-web-server modules.
 */
public enum WebMetrics {
  INSTANCE;

  private MetricRegistry _metrics;
  private Meter _webGetCall;
  private Meter _webPostCall;

  // How long does user log fetch take when user call fetch-log api.
  private AtomicLong _logFetchLatency = new AtomicLong(0L);

  WebMetrics() {
    _metrics = MetricsManager.INSTANCE.getRegistry();
    setupAllMetrics();
  }

  private void setupAllMetrics() {
    _webGetCall = addMeter("Web-Get-Call-Meter");
    _webPostCall = addMeter("Web-Post-Call-Meter");
    addLongGauge("fetchLogLatency", _logFetchLatency);
  }

  public Meter addMeter(String name) {
    Meter curr = _metrics.meter(name);
    _metrics.register(name + "-gauge", (Gauge<Double>) curr::getOneMinuteRate);
    return curr;
  }

  public void addLongGauge(String name, AtomicLong ai) {
    _metrics.register(name, (Gauge<Long>) ai::get);
  }

  public void markWebGetCall() {

    /*
     * Two reasons that we don't make this function call synchronized:
     * 1). code hale metrics deals with concurrency internally;
     * 2). mark is basically a math addition operation, which should not cause race condition issue.
     */
    _webGetCall.mark();
  }

  public void markWebPostCall() {

    _webPostCall.mark();
  }

  public void setFetchLogLatency(long milliseconds) {
    _logFetchLatency.set(milliseconds);
  }
}
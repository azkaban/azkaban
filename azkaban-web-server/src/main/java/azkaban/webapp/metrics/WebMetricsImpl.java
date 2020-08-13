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

package azkaban.webapp.metrics;

import azkaban.metrics.MetricsManager;
import azkaban.utils.Props;
import com.codahale.metrics.Meter;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class in charge of collecting metrics of the azkaban-web-server module.
 */
public class WebMetricsImpl implements WebMetrics {

  private static final Logger logger = LoggerFactory.getLogger(WebMetricsImpl.class);
  private final MetricsManager metricsManager;
  private final Meter webGetCall;
  private final Meter webPostCall;

  // How long does user log fetch take when user call fetch-log api.
  private final AtomicLong logFetchLatency = new AtomicLong(0L);

  @Inject
  public WebMetricsImpl(final MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
    this.webGetCall = this.metricsManager.addMeter("Web-Get-Call-Meter");
    this.webPostCall = this.metricsManager.addMeter("Web-Post-Call-Meter");
    this.metricsManager.addGauge("fetchLogLatency", this.logFetchLatency::get);
  }

  @Override
  public void setUp(final DataProvider dataProvider) {
    logger.info("Setting up Web Server metrics.");
    this.metricsManager
        .addGauge("JETTY-NumIdleThreads", dataProvider::getNumberOfIdleServerThreads);
    this.metricsManager.addGauge("JETTY-NumTotalThreads", dataProvider::getNumberOfServerThreads);
    this.metricsManager.addGauge("JETTY-NumQueueSize", dataProvider::getServerJobsQueueSize);

    this.metricsManager.addGauge("WEB-NumQueuedFlows", dataProvider::getNumberOfQueuedFlows);
    this.metricsManager.addGauge("WEB-NumAgedQueuedFlows",
        dataProvider::getNumberOfAgedQueuedFlows);
    this.metricsManager.addGauge("WEB-NumRunningFlows", dataProvider::getNumberOfRunningFlows);

    this.metricsManager.addGauge("session-count", dataProvider::getNumberOfCurrentSessions);
  }

  @Override
  public void startReporting(final Props props) {
    logger.info("Start reporting Web Server metrics.");
    this.metricsManager.startReporting("AZ-WEB", props);
  }

  /**
   * Mark the occurrence of a GET call
   *
   * This method should be Thread Safe. Two reasons that we don't make this function call
   * synchronized: 1). drop wizard metrics deals with concurrency internally; 2). mark is basically
   * a math addition operation, which should not cause race condition issue.
   */
  @Override
  public void markWebGetCall() {
    this.webGetCall.mark();
  }

  @Override
  public void markWebPostCall() {
    this.webPostCall.mark();
  }

  @Override
  public void setFetchLogLatency(final long milliseconds) {
    this.logFetchLatency.set(milliseconds);
  }

}

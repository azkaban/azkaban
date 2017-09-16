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

package azkaban.metrics;

import com.codahale.metrics.Meter;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * This singleton class CommonMetrics is in charge of collecting varieties of metrics which are
 * accessed in both web and exec modules. That said, these metrics will be exposed in both Web
 * server and executor.
 */
@Singleton
public class CommonMetrics {

  private final AtomicLong OOMWaitingJobCount = new AtomicLong(0L);
  private final MetricsManager metricsManager;
  private Meter flowFailMeter;
  private Meter dispatchFailMeter;
  private Meter dispatchSuccessMeter;
  private Meter sendEmailFailMeter;
  private Meter sendEmailSuccessMeter;

  @Inject
  public CommonMetrics(final MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
    setupAllMetrics();
  }

  private void setupAllMetrics() {
    this.flowFailMeter = this.metricsManager.addMeter("flow-fail-meter");
    this.dispatchFailMeter = this.metricsManager.addMeter("dispatch-fail-meter");
    this.dispatchSuccessMeter = this.metricsManager.addMeter("dispatch-success-meter");
    this.sendEmailFailMeter = this.metricsManager.addMeter("send-email-fail-meter");
    this.sendEmailSuccessMeter = this.metricsManager.addMeter("send-email-success-meter");
    this.metricsManager.addGauge("OOM-waiting-job-count", this.OOMWaitingJobCount::get);
  }


  /**
   * Mark flowFailMeter when a flow is considered as FAILED. This method could be called by Web
   * Server or Executor, as they both detect flow failure.
   */
  public void markFlowFail() {
    this.flowFailMeter.mark();
  }

  /**
   * Mark dispatchFailMeter when web server fails to dispatch a flow to executor.
   */
  public void markDispatchFail() {
    this.dispatchFailMeter.mark();
  }

  /**
   * Mark dispatchSuccessMeter when web server successfully dispatches a flow to executor.
   */
  public void markDispatchSuccess() {
    this.dispatchSuccessMeter.mark();
  }

  /**
   * Mark sendEmailFailMeter when an email fails to be sent out.
   */
  public void markSendEmailFail() {
    this.sendEmailFailMeter.mark();
  }

  /**
   * Mark sendEmailSuccessMeter when an email is sent out successfully.
   */
  public void markSendEmailSuccess() {
    this.sendEmailSuccessMeter.mark();
  }

  /**
   * Mark the occurrence of an job waiting event due to OOM
   */
  public void incrementOOMJobWaitCount() {
    this.OOMWaitingJobCount.incrementAndGet();
  }

  /**
   * Unmark the occurrence of an job waiting event due to OOM
   */
  public void decrementOOMJobWaitCount() {
    this.OOMWaitingJobCount.decrementAndGet();
  }

}

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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * This singleton class CommonMetrics is in charge of collecting varieties of metrics which are
 * accessed in both web and exec modules. That said, these metrics will be exposed in both Web
 * server and executor.
 */
@Singleton
public class CommonMetrics {
  public static final String FLOW_FAIL_METER_NAME = "flow-fail-meter";
  public static final String DISPATCH_FAIL_METER_NAME = "dispatch-fail-meter";
  public static final String DISPATCH_SUCCESS_METER_NAME = "dispatch-success-meter";
  public static final String SEND_EMAIL_FAIL_METER_NAME = "send-email-fail-meter";
  public static final String SEND_EMAIL_SUCCESS_METER_NAME = "send-email-success-meter";
  public static final String SUBMIT_FLOW_SUCCESS_METER_NAME = "submit-flow-success-meter";
  public static final String SUBMIT_FLOW_FAIL_METER_NAME = "submit-flow-fail-meter";
  public static final String SUBMIT_FLOW_SKIP_METER_NAME = "submit-flow-skip-meter";
  public static final String OOM_WAITING_JOB_COUNT_NAME = "OOM-waiting-job-count";
  public static final String UPLOAD_FAT_PROJECT_METER_NAME = "upload-fat-project-meter";
  public static final String UPLOAD_THIN_PROJECT_METER_NAME = "upload-thin-project-meter";

  private Counter OOMWaitingJobCount;
  private final MetricsManager metricsManager;
  private Meter flowFailMeter;
  private Meter dispatchFailMeter;
  private Meter dispatchSuccessMeter;
  private Meter sendEmailFailMeter;
  private Meter sendEmailSuccessMeter;
  private Meter submitFlowSuccessMeter;
  private Meter submitFlowFailMeter;
  private Meter submitFlowSkipMeter;
  private Meter uploadFatProjectMeter;
  private Meter uploadThinProjectMeter;

  @Inject
  public CommonMetrics(final MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
    setupAllMetrics();
  }

  private void setupAllMetrics() {
    this.flowFailMeter = this.metricsManager.addMeter(FLOW_FAIL_METER_NAME);
    this.dispatchFailMeter = this.metricsManager.addMeter(DISPATCH_FAIL_METER_NAME);
    this.dispatchSuccessMeter = this.metricsManager.addMeter(DISPATCH_SUCCESS_METER_NAME);
    this.sendEmailFailMeter = this.metricsManager.addMeter(SEND_EMAIL_FAIL_METER_NAME);
    this.sendEmailSuccessMeter = this.metricsManager.addMeter(SEND_EMAIL_SUCCESS_METER_NAME);
    this.submitFlowSuccessMeter = this.metricsManager.addMeter(SUBMIT_FLOW_SUCCESS_METER_NAME);
    this.submitFlowFailMeter = this.metricsManager.addMeter(SUBMIT_FLOW_FAIL_METER_NAME);
    this.submitFlowSkipMeter = this.metricsManager.addMeter(SUBMIT_FLOW_SKIP_METER_NAME);
    this.OOMWaitingJobCount = this.metricsManager.addCounter(OOM_WAITING_JOB_COUNT_NAME);
    this.uploadFatProjectMeter = this.metricsManager.addMeter(UPLOAD_FAT_PROJECT_METER_NAME);
    this.uploadThinProjectMeter = this.metricsManager.addMeter(UPLOAD_THIN_PROJECT_METER_NAME);
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
   * Mark submitFlowSuccessMeter when a flow is submitted for execution successfully.
   */
  public void markSubmitFlowSuccess() {
    this.submitFlowSuccessMeter.mark();
  }

  /**
   * Mark submitFlowFailMeter when a flow submitted for execution is skipped.
   */
  public void markSubmitFlowSkip() {
    this.submitFlowSkipMeter.mark();
  }

  /**
   * Mark submitFlowFailMeter when a flow fails to be submitted for execution.
   */
  public void markSubmitFlowFail() {
    this.submitFlowFailMeter.mark();
  }

  /**
   * Mark uploadFatProjectMeter when a fat project zip is uploaded to the web server.
   */
  public void markUploadFatProject() { this.uploadFatProjectMeter.mark(); }

  /**
   * Mark uploadThinProjectMeter when a thin project zip is uploaded to the web server.
   */
  public void markUploadThinProject() { this.uploadThinProjectMeter.mark(); }

  /**
   * Mark the occurrence of a job waiting event due to OOM
   */
  public void incrementOOMJobWaitCount() {
    this.OOMWaitingJobCount.inc();
  }

  /**
   * Unmark the occurrence of a job waiting event due to OOM
   */
  public void decrementOOMJobWaitCount() {
    this.OOMWaitingJobCount.dec();
  }
}

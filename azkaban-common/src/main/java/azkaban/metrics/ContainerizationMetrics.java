/*
 * Copyright 2021 LinkedIn Corp.
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

import azkaban.utils.Props;
import java.util.concurrent.TimeUnit;

/**
 * Defines all the metrics emitted by containerized executions
 */
public interface ContainerizationMetrics {

  void setUp();

  void startReporting(final Props props);

  /**
   * @return isInitialized status
   */
  public boolean isInitialized();

  /**
   *  Record the number of pod whose application containers exited without errors
   */
  void markPodCompleted();

  /**
   * Record the number of pod creation requests received by Kubernetes api-server
   */
  void markPodRequested();

  /**
   * Record the number of pod scheduled
   */
  void markPodScheduled();

  /**
   * Record the number of pod whose init containers are executing
   */
  void markInitContainerRunning();

  /**
   * Record the number of pod who has at least 1 application container started
   */
  void markAppContainerStarting();

  /**
   * Record the number of pod whose application containers all started
   */
  void markPodReady();

  /**
   * Record the number of pod failed during initialization
   */
  void markPodInitFailure();

  /**
   * Record the number of pod failed during application containers running
   */
  void markPodAppFailure();

  /**
   * Update a histogram of time durations from pod ready to pod preparing
   * @param time
   */
  void addTimeToDispatch(final long time);

  /**
   * Record a flow dispatched to executor
   */
  void markFlowSubmitToExecutor();

  /**
   * Record a flow dispatched to container
   */
  void markFlowSubmitToContainer();

  /**
   * Record number of flow executions stopped due to container infra failure
   */
  void markExecutionStopped();

  /**
   * Record number of flow executions stopped due to app container OOM killed
   */
  void markOOMKilled();

  /**
   * Record number of container dispatch failure
   */
  void markContainerDispatchFail();

  /**
   * Record number of vpa recommender get recommendation failure
   */
  void markVPARecommenderFail();

  /**
   * Record number of get yarn application failure
   */
  void markYarnGetApplicationsFail();

  /**
   * Record number of killing yarn application failure
   */
  void markYarnApplicationKillFail(long n);

  void sendCleanupContainerHeartBeat();

  void sendCleanupStaleFlowHeartBeat();

  void addCleanupStaleStatusFlowNumber(String status, long n);

  void sendCleanupYarnApplicationHeartBeat();

  void recordCleanupStaleFlowTimer(long duration, TimeUnit unit);

  void recordCleanupContainerTimer(long duration, TimeUnit unit);

  void recordCleanupYarnApplicationTimer(long duration, TimeUnit unit);
}

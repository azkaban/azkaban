/*
 * Copyright 2022 LinkedIn Corp.
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

package azkaban.executor;

/**
 * Defines operations common to all flow execution controllers.
 */
public interface IFlowRunnerManager {

  void pauseFlow(int execId, String user) throws ExecutorManagerException;

  void resumeFlow(final int execId, String user) throws ExecutorManagerException;

  void cancelFlow(int execId, String user) throws ExecutorManagerException;

  void cancelJobBySLA(int execId, String jobId) throws ExecutorManagerException;

  /**
   * Attempts to retry the failed jobs in a running execution.
   */
  void retryFailures(int execId, String user) throws ExecutorManagerException;
}

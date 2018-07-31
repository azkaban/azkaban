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

package azkaban.jobExecutor;

import azkaban.utils.Props;

/**
 * Raw job interface.
 *
 * A job is unit of work to perform.
 *
 * A job is required to have a constructor Job(String jobId, Props props)
 */
public interface Job {

  /**
   * Returns a unique(should be checked in xml) string name/id for the Job.
   *
   * @return the id
   */
  public String getId();

  /**
   * Run the job. In general this method can only be run once. Must either succeed or throw an
   * exception.
   *
   * @throws Exception the exception
   */
  public void run() throws Exception;

  /**
   * Best effort attempt to cancel the job.
   *
   * @throws Exception If cancel fails
   */
  public void cancel() throws Exception;

  /**
   * Returns a progress report between [0 - 1.0] to indicate the percentage complete
   *
   * @return the progress
   * @throws Exception If getting progress fails
   */
  public double getProgress() throws Exception;

  /**
   * Get the generated properties from this job.
   *
   * @return the job generated properties
   */
  public Props getJobGeneratedProperties();

  /**
   * Gets job props.
   *
   * @return the job props
   */
  public default Props getJobProps() {
    return new Props();
  }

  /**
   * Determine if the job was cancelled.
   *
   * @return the boolean
   */
  public boolean isCanceled();
}

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
 * This interface defines a Raw Job interface. Each job defines
 * <ul>
 * <li>Job Type : {HADOOP, UNIX, JAVA, SUCCESS_TEST, CONTROLLER}</li>
 * <li>Job ID/Name : {String}</li>
 * <li>Arguments: Key/Value Map for Strings</li>
 * </ul>
 *
 * A job is required to have a constructor Job(String jobId, Props props)
 */

public interface Job {

  /**
   * Returns a unique(should be checked in xml) string name/id for the Job.
   *
   * @return
   */
  public String getId();

  /**
   * Run the job. In general this method can only be run once. Must either
   * succeed or throw an exception.
   */
  public void run() throws Exception;

  /**
   * Best effort attempt to cancel the job.
   *
   * @throws Exception If cancel fails
   */
  public void cancel() throws Exception;

  /**
   * Returns a progress report between [0 - 1.0] to indicate the percentage
   * complete
   *
   * @throws Exception If getting progress fails
   */
  public double getProgress() throws Exception;

  /**
   * Get the generated properties from this job.
   *
   * @return
   */
  public Props getJobGeneratedProperties();

  /**
   * Determine if the job was cancelled.
   *
   * @return
   */
  public boolean isCanceled();
}

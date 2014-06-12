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

import org.apache.log4j.Logger;

import azkaban.utils.Props;

/**
 * A no-op job.
 */
public class NoopJob implements Job {
  private String jobId;

  public NoopJob(String jobid, Props props, Props jobProps, Logger log) {
    this.jobId = jobid;
  }

  @Override
  public String getId() {
    return this.jobId;
  }

  @Override
  public void run() throws Exception {
  }

  @Override
  public void cancel() throws Exception {
  }

  @Override
  public double getProgress() throws Exception {
    return 0;
  }

  @Override
  public Props getJobGeneratedProperties() {
    return new Props();
  }

  @Override
  public boolean isCanceled() {
    return false;
  }
}

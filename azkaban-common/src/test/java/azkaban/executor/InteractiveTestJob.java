/*
 * Copyright 2014 LinkedIn Corp.
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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;

public class InteractiveTestJob extends AbstractProcessJob {
  private static ConcurrentHashMap<String, InteractiveTestJob> testJobs =
      new ConcurrentHashMap<String, InteractiveTestJob>();
  private Props generatedProperties = new Props();
  private boolean isWaiting = true;
  private boolean succeed = true;

  public static InteractiveTestJob getTestJob(String name) {
    return testJobs.get(name);
  }

  public static void clearTestJobs() {
    testJobs.clear();
  }

  public InteractiveTestJob(String jobId, Props sysProps, Props jobProps,
      Logger log) {
    super(jobId, sysProps, jobProps, log);
  }

  @Override
  public void run() throws Exception {
    String nestedFlowPath =
        this.getJobProps().get(CommonJobProperties.NESTED_FLOW_PATH);
    String groupName = this.getJobProps().getString("group", null);
    String id = nestedFlowPath == null ? this.getId() : nestedFlowPath;
    if (groupName != null) {
      id = groupName + ":" + id;
    }
    testJobs.put(id, this);

    while (isWaiting) {
      synchronized (this) {
        try {
          wait(30000);
        } catch (InterruptedException e) {
        }

        if (!isWaiting) {
          if (!succeed) {
            throw new RuntimeException("Forced failure of " + getId());
          } else {
            info("Job " + getId() + " succeeded.");
          }
        }
      }
    }
  }

  public void failJob() {
    synchronized (this) {
      succeed = false;
      isWaiting = false;
      this.notify();
    }
  }

  public void succeedJob() {
    synchronized (this) {
      succeed = true;
      isWaiting = false;
      this.notify();
    }
  }

  public void succeedJob(Props generatedProperties) {
    synchronized (this) {
      this.generatedProperties = generatedProperties;
      succeed = true;
      isWaiting = false;
      this.notify();
    }
  }

  @Override
  public Props getJobGeneratedProperties() {
    return generatedProperties;
  }

  @Override
  public void cancel() throws InterruptedException {
    info("Killing job");
    failJob();
  }
}

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

import static azkaban.flow.CommonJobProperties.JOB_ATTEMPT;
import static org.junit.Assert.assertNotNull;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;
import java.io.File;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class InteractiveTestJob extends AbstractProcessJob {

  public static final String JOB_ID_PREFIX = "InteractiveTestJob.jobIdPrefix";
  private static final ConcurrentHashMap<String, InteractiveTestJob> testJobs =
      new ConcurrentHashMap<>();
  private static volatile boolean quickSuccess = false;
  private Props generatedProperties = new Props();
  private volatile boolean isWaiting = true;
  private volatile boolean succeed = true;
  private boolean ignoreCancel = false;

  public InteractiveTestJob(final String jobId, final Props sysProps, final Props jobProps,
      final Logger log) {
    super(jobId, sysProps, jobProps, log);
  }

  public static InteractiveTestJob getTestJob(final String name) {
    for (int i = 0; i < 1000; i++) {
      if (testJobs.containsKey(name)) {
        return testJobs.get(name);
      }
      synchronized (testJobs) {
        try {
          InteractiveTestJob.testJobs.wait(10L);
        } catch (final InterruptedException e) {
          i--;
        }
      }
    }
    throw new IllegalStateException(name + " wasn't added in testJobs map");
  }

  public static Collection<String> getTestJobNames() {
    return testJobs.keySet();
  }

  public static void clearTestJobs() {
    testJobs.clear();
  }

  public static void clearTestJobs(final String... names) {
    for (final String name : names) {
      assertNotNull(testJobs.remove(name));
    }
  }

  public static void setQuickSuccess(final boolean quickSuccess) {
    InteractiveTestJob.quickSuccess = quickSuccess;
  }

  public static void resetQuickSuccess() {
    InteractiveTestJob.quickSuccess = false;
  }

  @Override
  public void run() throws Exception {
    final File[] propFiles = initPropsFiles();
    final String nestedFlowPath =
        this.getJobProps().get(CommonJobProperties.NESTED_FLOW_PATH);
    final String jobIdPrefix = this.getJobProps().getString(JOB_ID_PREFIX, null);
    String id = nestedFlowPath == null ? this.getId() : nestedFlowPath;
    if (jobIdPrefix != null) {
      id = jobIdPrefix + ":" + id;
    }
    testJobs.put(id, this);
    synchronized (testJobs) {
      testJobs.notifyAll();
    }
    if (quickSuccess) {
      return;
    }

    if (this.jobProps.getBoolean("fail", false)) {
      final int passRetry = this.jobProps.getInt("passRetry", -1);
      if (passRetry > 0 && passRetry < this.jobProps.getInt(JOB_ATTEMPT)) {
        generateProperties(propFiles[1]);
        succeedJob();
      } else {
        failJob();
      }
    }
    if (!this.succeed) {
      throw new RuntimeException("Forced failure of " + getId());
    }

    while (this.isWaiting) {
      synchronized (this) {
        final int waitMillis = this.jobProps.getInt("seconds", 10) * 1000;
        if (waitMillis > 0) {
          try {
            wait(waitMillis);
          } catch (final InterruptedException e) {
          }
        }
        if (this.jobProps.containsKey("fail")) {
          generateProperties(propFiles[1]);
          succeedJob();
        }

        if (!this.isWaiting) {
          if (!this.succeed) {
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
      this.succeed = false;
      this.isWaiting = false;
      this.notify();
    }
  }

  public void succeedJob() {
    synchronized (this) {
      this.succeed = true;
      this.isWaiting = false;
      this.notify();
    }
  }

  public void succeedJob(final Props generatedProperties) {
    synchronized (this) {
      this.generatedProperties = generatedProperties;
      this.succeed = true;
      this.isWaiting = false;
      this.notify();
    }
  }

  public void ignoreCancel() {
    synchronized (this) {
      this.ignoreCancel = true;
    }
  }

  @Override
  public Props getJobGeneratedProperties() {
    return this.generatedProperties;
  }

  @Override
  public void cancel() throws InterruptedException {
    info("Killing job");
    if (!this.ignoreCancel) {
      failJob();
    }
  }
}

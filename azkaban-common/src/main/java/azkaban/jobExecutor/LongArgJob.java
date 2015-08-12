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

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import azkaban.utils.Props;
import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.jobExecutor.utils.process.AzkabanProcessBuilder;

/**
 * A job that passes all the job properties as command line arguments in "long"
 * format, e.g. --key1 value1 --key2 value2 ...
 */
public abstract class LongArgJob extends AbstractProcessJob {

  private static final long KILL_TIME_MS = 5000;
  private final AzkabanProcessBuilder builder;
  private volatile AzkabanProcess process;

  public LongArgJob(String jobid, String[] command, Props sysProps,
      Props jobProps, Logger log) {
    this(jobid, command, sysProps, jobProps, log, new HashSet<String>(0));
  }

  public LongArgJob(String jobid, String[] command, Props sysProps,
      Props jobProp, Logger log, Set<String> suppressedKeys) {
    super(jobid, sysProps, jobProp, log);

    this.builder =
        new AzkabanProcessBuilder(command)
            .setEnv(getJobProps().getMapByPrefix(ENV_PREFIX))
            .setWorkingDir(getCwd()).setLogger(getLog());
    appendProps(suppressedKeys);
  }

  public void run() throws Exception {
    try {
      resolveProps();
    } catch (Exception e) {
      error("Bad property definition! " + e.getMessage());
    }

    long startMs = System.currentTimeMillis();
    info("Command: " + builder.getCommandString());
    if (builder.getEnv().size() > 0) {
      info("Environment variables: " + builder.getEnv());
    }
    info("Working directory: " + builder.getWorkingDir());

    File[] propFiles = initPropsFiles();

    // print out the Job properties to the job log.
    this.logJobProperties();

    boolean success = false;
    this.process = builder.build();
    try {
      this.process.run();
      success = true;
    } catch (Exception e) {
      for (File file : propFiles) {
        if (file != null && file.exists()) {
          file.delete();
        }
      }
      throw new RuntimeException(e);
    } finally {
      this.process = null;
      info("Process completed " + (success ? "successfully" : "unsuccessfully")
          + " in " + ((System.currentTimeMillis() - startMs) / 1000)
          + " seconds.");
    }

    // Get the output properties from this job.
    generateProperties(propFiles[1]);

    for (File file : propFiles) {
      if (file != null && file.exists()) {
        file.delete();
      }
    }
  }

  /**
   * This gives access to the process builder used to construct the process. An
   * overriding class can use this to add to the command being executed.
   */
  protected AzkabanProcessBuilder getBuilder() {
    return this.builder;
  }

  @Override
  public void cancel() throws InterruptedException {
    if (process == null) {
      throw new IllegalStateException("Not started.");
    }

    boolean killed = process.softKill(KILL_TIME_MS, TimeUnit.MILLISECONDS);
    if (!killed) {
      warn("Kill with signal TERM failed. Killing with KILL signal.");
      process.hardKill();
    }
  }

  @Override
  public double getProgress() {
    return process != null && process.isComplete() ? 1.0 : 0.0;
  }

  private void appendProps(Set<String> suppressed) {
    AzkabanProcessBuilder builder = this.getBuilder();
    Props props = getJobProps();
    for (String key : props.getKeySet()) {
      if (!suppressed.contains(key)) {
        builder.addArg("--" + key, props.get(key));
      }
    }
  }
}
